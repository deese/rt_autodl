#!/usr/bin/env python3
"""Main entry point for rt_autodl."""

import argparse
import concurrent.futures
import os
from typing import Any, Dict, Tuple

from rich.console import Console
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn, TransferSpeedColumn

try:
    from .config import load_config
    from .ftps_client import ftps_get, ftps_plan_from_files
    from .rutorrent_client import connect_rutorrent, is_completed, list_by_label, relabel
    from .secrets import maybe_load_dotenv, resolve_secret
    from .utils import DRY_RUN, ensure_dir, set_flags, vprint
except ImportError:
    from config import load_config
    from ftps_client import ftps_get, ftps_plan_from_files
    from rutorrent_client import connect_rutorrent, is_completed, list_by_label, relabel
    from secrets import maybe_load_dotenv, resolve_secret
    from utils import DRY_RUN, ensure_dir, set_flags, vprint


def process_torrent(cfg: Dict[str, Any], rt, t: Dict[str, Any], mapping: Dict[str, Any], console: Console, progress: Progress) -> None:
    """Process a single torrent for download and relabeling."""
    dst_label = mapping["target"]

    thash = t.get("hash") or t.get("info_hash") or ""
    name  = t.get("name") or thash or "torrent"
    if not thash:
        console.print(f"[yellow][SKIP][/yellow] {name}: missing hash")
        return
    if not is_completed(t):
        console.print(f"[cyan][SKIP][/cyan] {name}: not completed")
        return

    files = t.get("files") or []
    if not files:
        console.print(f"[yellow][WARN][/yellow] {name}: no file list; cannot plan FTPS paths")
        return

    s = cfg["sftp"]
    dest_root = mapping.get("dest_dir") or s["dest_dir"]
    plan = ftps_plan_from_files(t, s.get("ftp_root", "/"), s.get("rtorrent_root"))
    if not plan:
        console.print(f"[yellow][WARN][/yellow] {name}: nothing to transfer (ftps plan empty)")
        return

    ensure_dir(dest_root)
    console.print(f"[green][PROC][/green] {name}: {len(plan)} files -> {dest_root} [ftps]")
    vprint(console, f"Plan sample: {plan[:3]}")

    # Optional per-file concurrency
    file_workers = int(s.get("ftps_file_concurrency", 1))

    def _one(item: Tuple[str,str,int]) -> None:
        remote, rel, size = item
        dst = os.path.normpath(os.path.join(dest_root, rel))
        if DRY_RUN:
            vprint(console, f"[FTPS] {remote} -> {dst} ({size} bytes)")
            return

        # If file exists already:
        if os.path.exists(dst):
            # If we know the size and it matches, skip without progress bar
            try:
                if size > 0 and os.path.getsize(dst) == size:
                    vprint(console, f"[FTPS] exists same size -> skip (no bar): {dst}")
                    return
            except Exception:
                pass
            # Unknown size: let ftps_get probe & skip without creating a bar
            ftps_get(cfg, remote, dst, size, progress, task_id=None)
            return

        # Not existing: create a bar and download
        task = progress.add_task(f"[white]{rel}", total=size if size > 0 else None)
        try:
            ftps_get(cfg, remote, dst, size, progress, task)
        finally:
            progress.remove_task(task)

    if file_workers > 1 and len(plan) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=file_workers) as ex:
            list(ex.map(_one, plan))
    else:
        for item in plan:
            _one(item)

    # Relabel after transfers (always attempt, even if downloads were skipped)
    try:
        relabel(rt, thash, dst_label, cfg, console)
        console.print(f"[magenta][LABEL][/magenta] {name}: {mapping['source']} -> {dst_label}")
    except Exception as e:
        console.print(f"[red][ERR][/red] {name}: relabel {mapping['source']} -> {dst_label} failed: {e}")
        vprint(console, f"Label error for {thash}: {repr(e)}")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to JSON config")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    parser.add_argument("--dry-run", action="store_true", help="Only print planned actions, do not transfer")
    args = parser.parse_args()

    # Set global flags
    set_flags(verbose=bool(args.verbose), dry_run=bool(args.dry_run))

    console = Console()
    if args.verbose:
        console.log("Verbose enabled")

    cfg = load_config(args.config)
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

    rt = connect_rutorrent(cfg["rutorrent"]["uri"])

    # Process all label mappings
    all_torrents_found = False
    
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TransferSpeedColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    ) as progress:
        for mapping in cfg["label_mappings"]:
            source_label = mapping["source"]
            torrents = list_by_label(rt, source_label)
            
            if not torrents:
                console.print(f"[yellow]No torrents with label '{source_label}'.[/yellow]")
                continue
                
            all_torrents_found = True
            console.print(f"[blue]Processing {len(torrents)} torrents with label '{source_label}'[/blue]")
            
            for t in torrents:
                process_torrent(cfg, rt, t, mapping, console, progress)
    
    if not all_torrents_found:
        console.print("[yellow]No torrents found for any configured labels.[/yellow]")


if __name__ == "__main__":
    main()