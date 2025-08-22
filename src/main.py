#!/usr/bin/env python3
"""Main entry point for rt_autodl."""

import argparse
import concurrent.futures
import os
import time
from typing import Any, Dict, Tuple

from rich.console import Console
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn, TransferSpeedColumn, SpinnerColumn

try:
    from .config import load_config
    from .ftps_client import ftps_get, ftps_plan_from_files
    from .rutorrent_client import connect_rutorrent, is_completed, list_by_label, relabel
    from .secrets import maybe_load_dotenv, resolve_secret
    from .utils import DRY_RUN, ensure_dir, set_flags, vprint
    from .logger import init_logger, get_logger, log_config_loaded, log_session_start, log_session_end
    from .stats import get_stats_tracker
    from .connection_pool import get_connection_pool, close_connection_pool
except ImportError:
    from config import load_config
    from ftps_client import ftps_get, ftps_plan_from_files
    from rutorrent_client import connect_rutorrent, is_completed, list_by_label, relabel
    from secrets import maybe_load_dotenv, resolve_secret
    from utils import DRY_RUN, ensure_dir, set_flags, vprint
    from logger import init_logger, get_logger, log_config_loaded, log_session_start, log_session_end
    from stats import get_stats_tracker
    from connection_pool import get_connection_pool, close_connection_pool


def process_torrent(cfg: Dict[str, Any], rt, t: Dict[str, Any], mapping: Dict[str, Any], console: Console, progress: Progress) -> bool:
    """Process a single torrent for download and relabeling. Returns True if successful."""
    logger = get_logger()
    stats = get_stats_tracker()
    
    dst_label = mapping["target"]
    thash = t.get("hash") or t.get("info_hash") or ""
    name  = t.get("name") or thash or "torrent"
    
    if not thash:
        logger.warning(f"Skipping {name}: missing hash", torrent_name=name)
        console.print(f"[yellow][SKIP][/yellow] {name}: missing hash")
        return False
        
    if not is_completed(t):
        logger.info(f"Skipping {name}: not completed", torrent_name=name, torrent_hash=thash)
        console.print(f"[cyan][SKIP][/cyan] {name}: not completed")
        return False

    files = t.get("files") or []
    if not files:
        logger.warning(f"Skipping {name}: no file list", torrent_name=name, torrent_hash=thash)
        console.print(f"[yellow][WARN][/yellow] {name}: no file list; cannot plan FTPS paths")
        return False

    s = cfg["sftp"]
    dest_root = mapping.get("dest_dir") or s["dest_dir"]
    plan = ftps_plan_from_files(t, s.get("ftp_root", "/"), s.get("rtorrent_root"))
    if not plan:
        logger.warning(f"Skipping {name}: empty transfer plan", torrent_name=name, torrent_hash=thash)
        console.print(f"[yellow][WARN][/yellow] {name}: nothing to transfer (ftps plan empty)")
        return False

    ensure_dir(dest_root)
    
    # Calculate total size for statistics
    total_size = sum(size for _, _, size in plan)
    
    # Log transfer start
    logger.transfer_start(name, thash, len(plan), total_size, dest_root)
    console.print(f"[green][PROC][/green] {name}: {len(plan)} files -> {dest_root} [ftps]")
    vprint(console, f"Plan sample: {plan[:3]}")
    
    stats.record_torrent_processed()

    # Optional per-file concurrency
    file_workers = int(s.get("ftps_file_concurrency", 1))
    transfer_success = True
    transfer_errors = []

    def _one(item: Tuple[str,str,int]) -> bool:
        nonlocal transfer_success
        remote, rel, size = item
        dst = os.path.normpath(os.path.join(dest_root, rel))
        
        if DRY_RUN:
            vprint(console, f"[FTPS] {remote} -> {dst} ({size} bytes)")
            return True

        # Start tracking this transfer
        transfer_id = stats.start_transfer(rel, thash, size)
        
        try:
            # If file exists already:
            if os.path.exists(dst):
                # If we know the size and it matches, skip without progress bar
                try:
                    if size > 0 and os.path.getsize(dst) == size:
                        logger.debug(f"Skipping existing file: {rel}", 
                                   torrent_hash=thash, file_path=rel, size=size)
                        vprint(console, f"[FTPS] exists same size -> skip (no bar): {dst}")
                        stats.skip_transfer(rel, thash, size)
                        stats.complete_transfer(transfer_id, success=True)
                        return True
                except Exception:
                    pass
                # Unknown size: let ftps_get probe & skip without creating a bar
                ftps_get(cfg, remote, dst, size, progress, task_id=None)
                stats.complete_transfer(transfer_id, success=True)
                return True

            # Not existing: create a bar and download
            task = progress.add_task(f"[white]{rel}", total=size if size > 0 else None)
            try:
                start_time = time.time()
                ftps_get(cfg, remote, dst, size, progress, task)
                duration = time.time() - start_time
                
                # Log successful transfer
                logger.transfer_complete(thash, rel, size, duration)
                stats.complete_transfer(transfer_id, success=True)
                return True
                
            except Exception as e:
                duration = time.time() - start_time if 'start_time' in locals() else 0
                error_msg = str(e)
                
                # Log transfer error
                logger.transfer_error(thash, rel, error_msg)
                stats.complete_transfer(transfer_id, success=False, error=error_msg)
                transfer_errors.append((rel, error_msg))
                return False
                
            finally:
                progress.remove_task(task)
                
        except Exception as e:
            error_msg = str(e)
            logger.transfer_error(thash, rel, error_msg)
            stats.complete_transfer(transfer_id, success=False, error=error_msg)
            transfer_errors.append((rel, error_msg))
            return False

    if file_workers > 1 and len(plan) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=file_workers) as ex:
            results = list(ex.map(_one, plan))
            transfer_success = all(results)
    else:
        for item in plan:
            if not _one(item):
                transfer_success = False

    # Relabel after transfers (always attempt, even if downloads were skipped)
    try:
        relabel(rt, thash, dst_label, cfg, console)
        logger.relabel_success(thash, name, mapping['source'], dst_label)
        console.print(f"[magenta][LABEL][/magenta] {name}: {mapping['source']} -> {dst_label}")
        
    except Exception as e:
        error_msg = str(e)
        logger.relabel_error(thash, name, mapping['source'], dst_label, error_msg)
        console.print(f"[red][ERR][/red] {name}: relabel {mapping['source']} -> {dst_label} failed: {e}")
        vprint(console, f"Label error for {thash}: {repr(e)}")
        transfer_success = False
    
    # Log summary for this torrent
    if transfer_errors:
        logger.warning(f"Torrent processing completed with errors: {name}", 
                      torrent_name=name, torrent_hash=thash, 
                      errors=len(transfer_errors), total_files=len(plan))
    elif transfer_success:
        logger.info(f"Torrent processing successful: {name}", 
                   torrent_name=name, torrent_hash=thash, files_count=len(plan))
    
    return transfer_success

def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="RT AutoDL - Automated torrent downloader")
    parser.add_argument("--config", required=True, help="Path to JSON config")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    parser.add_argument("--dry-run", action="store_true", help="Only print planned actions, do not transfer")
    parser.add_argument("--json-logs", action="store_true", help="Output structured JSON logs")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Set logging level")
    args = parser.parse_args()

    # Initialize logging
    logger = init_logger(
        level=args.log_level,
        json_output=args.json_logs,
        console_output=not args.json_logs  # Use console output unless JSON requested
    )
    
    # Initialize statistics tracking
    stats = get_stats_tracker()
    stats.start_session()
    
    # Set global flags
    set_flags(verbose=bool(args.verbose), dry_run=bool(args.dry_run))

    console = Console()
    if args.verbose:
        logger.debug("Verbose mode enabled")
        console.log("Verbose enabled")
    
    # Log session start
    log_session_start()

    try:
        cfg = load_config(args.config)
        log_config_loaded(args.config, len(cfg.get("label_mappings", [])))
        
        # Load dotenv (optional) and resolve secrets before connecting
        maybe_load_dotenv(cfg, console)
        
        # Resolve ruTorrent URI
        cfg["rutorrent"]["uri"] = resolve_secret(cfg["rutorrent"].get("uri"), cfg, console=console) or cfg["rutorrent"].get("uri")
        
        # Resolve FTPS user/password
        s = cfg.get("sftp", {})
        s["ftps_user"] = resolve_secret(s.get("ftps_user"), cfg, console=console) or s.get("ftps_user")
        s["ftps_password"] = resolve_secret(s.get("ftps_password"), cfg, username=s.get("ftps_user"), console=console) or s.get("ftps_password")
        # Reassign (in case dict was a shallow copy)
        cfg["sftp"] = s
        
        # Initialize connection pool
        pool = get_connection_pool(cfg)
        logger.info("Connection pool initialized", max_connections=pool.max_connections)
        
        # Connect to ruTorrent
        with logger.operation_timer("rutorrent_connect"):
            rt = connect_rutorrent(cfg["rutorrent"]["uri"])
            logger.info("Connected to ruTorrent", uri_host=cfg["rutorrent"]["uri"].split('@')[-1].split('/')[0] if '@' in cfg["rutorrent"]["uri"] else "unknown")
            
    except Exception as e:
        logger.error(f"Initialization failed: {e}", error=str(e))
        raise

    # Process all label mappings
    total_torrents = 0
    successful_torrents = 0
    
    try:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TransferSpeedColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeRemainingColumn(elapsed_when_finished=True),
            console=console,
            transient=False,
        ) as progress:
            with logger.operation_timer("torrent_processing"):
                for mapping in cfg["label_mappings"]:
                    source_label = mapping["source"]
                    
                    with logger.operation_timer("torrent_query", label=source_label):
                        torrents = list_by_label(rt, source_label)
                    
                    if not torrents:
                        logger.info(f"No torrents found for label: {source_label}", label=source_label)
                        console.print(f"[yellow]No torrents with label '{source_label}'.[/yellow]")
                        continue
                    
                    total_torrents += len(torrents)
                    logger.info(f"Processing torrents for label: {source_label}", 
                               label=source_label, torrent_count=len(torrents))
                    console.print(f"[blue]Processing {len(torrents)} torrents with label '{source_label}'[/blue]")
                    
                    for t in torrents:
                        if process_torrent(cfg, rt, t, mapping, console, progress):
                            successful_torrents += 1
        
        if total_torrents == 0:
            logger.warning("No torrents found for any configured labels")
            console.print("[yellow]No torrents found for any configured labels.[/yellow]")
        else:
            logger.info(f"Processing complete: {successful_torrents}/{total_torrents} successful", 
                       total_torrents=total_torrents, successful=successful_torrents)
    
    finally:
        # End session and log statistics
        stats.end_session()
        
        # Get final statistics
        session_summary = stats.get_session_summary()
        pool_stats = pool.get_pool_stats()
        
        # Log session summary
        log_session_end(
            total_torrents=total_torrents,
            success_count=successful_torrents,
            error_count=total_torrents - successful_torrents
        )
        
        logger.info("Session statistics", **session_summary)
        logger.info("Connection pool statistics", **pool_stats)
        
        # Display summary if not in JSON mode
        if not args.json_logs:
            console.print("\n[bold green]Session Summary[/bold green]")
            console.print(f"Duration: {session_summary['duration']:.1f}s")
            console.print(f"Torrents processed: {session_summary['torrents_processed']}")
            console.print(f"Files: {session_summary['files']['successful']}/{session_summary['files']['attempted']} successful ({session_summary['files']['success_rate']}%)")
            console.print(f"Data transferred: {stats.format_size(session_summary['bytes']['transferred'])}")
            console.print(f"Average speed: {stats.format_speed(session_summary['transfer_speed']['average'])}")
            console.print(f"Connection success rate: {session_summary['connections']['success_rate']}%")
            console.print(f"Connection pool hit rate: {session_summary['connections']['pool_hit_rate']}%")
        
        # Close connection pool
        close_connection_pool()
        logger.debug("Connection pool closed")


if __name__ == "__main__":
    main()
