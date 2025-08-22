#!/usr/bin/env python3
"""FTPS client functionality for rt_autodl."""

import concurrent.futures
import ftplib
import mmap
import os
import posixpath
import ssl
import threading
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.progress import Progress

try:
    from .utils import retry_on_failure, vprint, ensure_dir, posix_norm, join_posix
    from .connection_pool import get_connection_pool
    from .logger import get_logger
    from .stats import get_stats_tracker
except ImportError:
    from utils import retry_on_failure, vprint, ensure_dir, posix_norm, join_posix
    from connection_pool import get_connection_pool
    from logger import get_logger
    from stats import get_stats_tracker


def _normalize_filename(filename: str) -> str:
    """Normalize filename for comparison by handling Unicode normalization and encoding issues."""
    if not filename:
        return filename
    
    # First normalize Unicode to NFC form
    normalized = unicodedata.normalize('NFC', filename)
    
    # Handle common mojibake cases where UTF-8 was decoded as latin-1
    # This handles cases like "guardiÃ¡n" -> "guardián"
    try:
        # Try to detect if this might be mojibake by encoding as latin-1 and decoding as utf-8
        if 'Ã' in normalized:  # Common mojibake indicator
            latin1_bytes = normalized.encode('latin-1')
            utf8_decoded = latin1_bytes.decode('utf-8')
            # If successful and the result is different, use the corrected version
            if utf8_decoded != normalized:
                normalized = utf8_decoded
    except (UnicodeEncodeError, UnicodeDecodeError):
        # If conversion fails, keep the original normalized version
        pass
    
    return normalized


def _rel_from_frozen(frozen: str, rtorrent_root: Optional[str]) -> Optional[str]:
    """Return a POSIX relative path from frozen_path by stripping rtorrent_root if given; else None."""
    if not frozen:
        return None
    frozen = posix_norm(frozen)
    if rtorrent_root:
        base = posix_norm(rtorrent_root)
        try:
            rel = posixpath.relpath(frozen, base)
            if rel.startswith(".."):
                return None
            return rel
        except Exception:
            return None
    return None


def ftps_plan_from_files(torrent: Dict[str, Any], ftp_root: str, rtorrent_root: Optional[str]) -> List[Tuple[str, str, int]]:
    """
    Return a list of (remote_abs_path_under_ftp_root, rel_path_for_dest, size).
    - rel path: prefer file['path'] or file['name']; otherwise derive from frozen_path using rtorrent_root.
    - size: prefer file size fields; if missing AND it's a single-file torrent, fallback to torrent['bytes_total'].
    
    For multi-file torrents, use the torrent name as the base folder path.
    """
    files = torrent.get("files") or []
    is_single = len(files) == 1

    plan: List[Tuple[str, str, int]] = []
    ftp_root = posix_norm(ftp_root or "/")
    
    # For multi-file torrents, use the torrent name as the base folder
    torrent_name = torrent.get("name", "")
    
    for f in files:
        rel = f.get("path") or f.get("name")
        if not rel:
            rel = _rel_from_frozen(f.get("frozen_path") or "", rtorrent_root)
            if not rel:
                continue
        rel = rel.lstrip("/")  # enforce relative under ftp_root

        size = 0
        for key in ("size_bytes", "length", "size"):
            v = f.get(key)
            if isinstance(v, int) and v > 0:
                size = v
                break
        if size <= 0 and is_single:
            v = torrent.get("bytes_total") or torrent.get("size") or 0
            if isinstance(v, int) and v > 0:
                size = v

        # For multi-file torrents, prepend the torrent name to create the full remote path
        if not is_single and torrent_name:
            # Preserve the full relative path structure for multi-file torrents
            remote_rel = join_posix(torrent_name, rel)
            # Also preserve the folder structure in the destination path
            dest_rel = join_posix(torrent_name, rel)
        else:
            remote_rel = rel
            dest_rel = rel
            
        remote = join_posix(ftp_root, remote_rel)
        plan.append((remote, dest_rel, int(size)))
    return plan


@retry_on_failure(max_attempts=3, delay=2.0)
def ftps_connect(s: Dict[str, Any], ctx: ssl.SSLContext):
    """Establish FTPS connection (legacy function - use connection pool instead)."""
    timeout = int(s.get("ftps_timeout", 30))
    pasv = bool(s.get("ftps_pasv", True))
    try:
        ftp = ftplib.FTP_TLS(timeout=timeout, context=ctx)
    except TypeError:
        ftp = ftplib.FTP_TLS(timeout=timeout)  # older Python fallback
        ftp.context = ctx  # type: ignore[attr-defined]
    ftp.connect(s["ftps_host"], int(s.get("ftps_port", 21)))
    ftp.login(user=s["ftps_user"], passwd=(s.get("ftps_password") or ""))
    ftp.prot_p()
    ftp.set_pasv(pasv)
    try:
        ftp.voidcmd('TYPE I')  # ensure binary
    except Exception:
        pass
    return ftp


def _ftps_resolve_remote(cfg: Dict[str, Any], remote: str) -> Tuple[str, str, str, int]:
    """
    Try multiple candidate paths by progressively stripping leading components from the
    relative portion under ftp_root until we find one that exists. Returns (canonical_remote, rdir, rname, size).
    """
    pool = get_connection_pool(cfg)
    s = cfg["sftp"]
    
    with pool.get_connection() as ftp:
        try:
        ftp_root = posix_norm(s.get("ftp_root", "/"))
        remote = posix_norm(remote)

        # Derive relative path under ftp_root if possible
        rel = remote
        if remote.startswith(ftp_root.rstrip("/") + "/") or remote == ftp_root:
            rel = remote[len(ftp_root):].lstrip("/")
        else:
            # fallback: treat remote as a relative path already
            rel = remote.lstrip("/")

        parts = [p for p in rel.split("/") if p]
        tails = []
        for i in range(len(parts)):
            tails.append("/".join(parts[i:]))
        if not tails:
            tails = [rel]

        last_err = None
        for tail in tails:
            cand = join_posix(ftp_root, tail)
            rdir, rname = posixpath.split(cand)
            try:
                if rdir and rdir not in {"", "/"}:
                    ftp.cwd(rdir)
                try:
                    rsize = ftp.size(rname)
                except Exception:
                    try:
                        # Try MLSD first for structured listing with file sizes
                        file_entries = {}
                        try:
                            for entry_name, entry_facts in ftp.mlsd():
                                if entry_facts.get('type') == 'file':
                                    file_size = int(entry_facts.get('size', 0))
                                    file_entries[entry_name] = file_size
                        except (ftplib.error_perm, AttributeError):
                            # Fallback to NLST if MLSD not supported
                            names = ftp.nlst()
                            file_entries = {name: 0 for name in names}
                        
                        # Try exact match first
                        if rname in file_entries:
                            rsize = file_entries[rname]
                        else:
                            # Normalize the target filename for comparison
                            rname_norm = _normalize_filename(rname)
                            rname_lower = rname_norm.lower()
                            found = False
                            
                            for n, n_size in file_entries.items():
                                n_norm = _normalize_filename(n)
                                n_lower = n_norm.lower()
                                
                                # Try normalized exact match
                                if n_norm == rname_norm:
                                    rname = n  # Use the actual filename from server
                                    rsize = n_size
                                    found = True
                                    break
                                # Try case-insensitive normalized match
                                elif n_lower == rname_lower:
                                    rname = n  # Use the actual filename from server
                                    rsize = n_size
                                    found = True
                                    break
                                # Try path-based matches with normalization
                                elif n.endswith("/" + rname) or n_norm.endswith("/" + rname_norm):
                                    rname = n.split("/")[-1]  # Extract filename
                                    rsize = n_size
                                    found = True
                                    break
                                elif n_lower.endswith("/" + rname_lower):
                                    rname = n.split("/")[-1]  # Extract filename
                                    rsize = n_size
                                    found = True
                                    break
                            
                            # If still not found, try partial matching for nested files
                            if not found:
                                for n, n_size in file_entries.items():
                                    # Check if this is a directory that might contain our file
                                    if "/" not in n and n != rname:
                                        continue
                                    n_norm = _normalize_filename(n)
                                    n_lower = n_norm.lower()
                                    # Check if the filename appears anywhere in the path with normalization
                                    if (rname in n or rname_norm in n_norm or 
                                        rname_lower in n_lower):
                                        rname = n.split("/")[-1] if "/" in n else n
                                        rsize = n_size
                                        found = True
                                        break
                            if not found:
                                raise FileNotFoundError(f"{rname} not in listing")
                    except Exception as e:
                        last_err = e
                        continue
                if rsize is None:
                    rsize = 0
                return (cand, rdir, rname, int(rsize))
            except Exception as e:
                last_err = e
                continue
        if last_err:
            raise FileNotFoundError(f"No matching remote path for {remote}: {last_err}")
        raise FileNotFoundError(f"No matching remote path for {remote}")
        
        # Connection is automatically returned to pool by context manager


def ftps_get(cfg: Dict[str, Any], remote: str, dst: str, size_hint: int, progress: Progress, task_id=None) -> None:
    """
    FTPS download with optional multi-connection segmentation using REST + transfercmd.
    If the destination exists with same size, SKIP download (but caller still relabels).
    """
    logger = get_logger()
    stats = get_stats_tracker()
    s = cfg["sftp"]
    pool = get_connection_pool(cfg)
    
    ensure_dir(os.path.dirname(dst))
    remote = posix_norm(remote)
    
    logger.debug(f"Starting FTPS download: {remote} -> {dst}", 
                remote_path=remote, local_path=dst, size_hint=size_hint)

    # Resolve canonical path & probed size
    canonical_remote, rdir, rname, probed_size = _ftps_resolve_remote(cfg, remote)
    remote = canonical_remote

    # Fast skip if same-size destination exists
    if os.path.exists(dst):
        try:
            local_size = os.path.getsize(dst)
            if (probed_size > 0 and local_size == probed_size) or (size_hint > 0 and local_size == size_hint):
                logger.debug(f"Skipping existing file with same size: {dst}", 
                           local_path=dst, size=local_size)
                vprint(Console(), f"[FTPS] skip (exists same size): {dst}")
                progress.update(task_id, completed=max(probed_size, size_hint)) if task_id is not None else None
                return
        except Exception as e:
            logger.debug(f"Error checking existing file: {e}", local_path=dst)

    blocksize = int(s.get("ftps_blocksize", 262144))
    segments  = int(s.get("ftps_segments", 1))
    min_seg   = int(s.get("ftps_min_seg_size", 8 * 1024 * 1024))

    # Determine size for segmentation decision
    rsize = probed_size if probed_size > 0 else size_hint
    if rsize <= 0:
        with pool.get_connection() as ftp:
            try:
                if rdir and rdir not in {"", "/"}:
                    ftp.cwd(rdir)
                try:
                    rsize = ftp.size(rname) or 0
                except Exception:
                    rsize = 0
            except Exception as e:
                logger.warning(f"Could not determine file size: {e}", remote_path=remote)

    logger.debug(f"Transfer parameters: size={rsize}, segments={segments}, blocksize={blocksize}",
                remote_path=remote, size=rsize, segments=segments)
    
    # Single-stream path
    if rsize <= 0 or segments <= 1 or rsize < max(min_seg, segments * blocksize):
        logger.debug(f"Using single-stream transfer for {remote}", remote_path=remote)
        
        with pool.get_connection() as ftp:
            try:
                if rdir and rdir not in {"", "/"}:
                    ftp.cwd(rdir)
                done = 0
                tmp = dst + ".part"
                try:
                    if os.path.exists(tmp):
                        os.remove(tmp)
                except Exception:
                    pass
                    
                with open(tmp, "wb") as wf:
                    def _cb(chunk: bytes):
                        nonlocal done
                        wf.write(chunk)
                        done += len(chunk)
                        total = rsize if rsize > 0 else done
                        progress.update(task_id, completed=min(done, total)) if task_id is not None else None
                    
                    ftp.retrbinary(f"RETR {rname}", _cb, blocksize=blocksize)
                
                os.replace(tmp, dst)  # atomic replace on success
                logger.debug(f"Single-stream transfer completed: {done} bytes", 
                           remote_path=remote, bytes_transferred=done)
                           
            except Exception as e:
                logger.error(f"Single-stream transfer failed: {e}", remote_path=remote, error=str(e))
                raise
        return

    # Segmented path
    tmp = dst + ".part"
    try:
        if os.path.exists(tmp):
            os.remove(tmp)
    except Exception:
        pass
    with open(tmp, "wb") as f:
        f.truncate(rsize)
    f = open(tmp, "r+b")
    mm = mmap.mmap(f.fileno(), rsize, access=mmap.ACCESS_WRITE)

    ranges: List[Tuple[int,int]] = []
    seg_size = rsize // segments
    
    progress.update(task_id, total=rsize)

    for i in range(segments):
        start_off = i * seg_size
        end_off = rsize if i == segments - 1 else (i + 1) * seg_size
        ranges.append((start_off, end_off))

    errors: List[Exception] = []

    logger.debug(f"Using segmented transfer: {segments} segments", 
                remote_path=remote, segments=segments, total_size=rsize)
    
    def worker(start_off: int, end_off: int):
        try:
            with pool.get_connection() as ftp:
                try:
                    if rdir and rdir not in {"", "/"}:
                        ftp.cwd(rdir)
                    datasock = ftp.transfercmd(f"RETR {rname}", rest=start_off)
                    remaining = end_off - start_off
                    pos = start_off
                    
                    logger.debug(f"Starting segment: {start_off}-{end_off}", 
                               remote_path=remote, segment_start=start_off, segment_end=end_off)
                    
                    while remaining > 0:
                        chunk = datasock.recv(min(blocksize, remaining))
                        if not chunk:
                            break
                        mm[pos:pos+len(chunk)] = chunk
                        pos += len(chunk)
                        remaining -= len(chunk)
                        progress.update(task_id, advance=len(chunk)) if task_id is not None else None
                    
                    try:
                        datasock.close()
                    finally:
                        try: ftp.voidresp()
                        except Exception: pass
                        
                except Exception as e:
                    logger.error(f"Segment {start_off}-{end_off} failed: {e}", 
                               remote_path=remote, segment_start=start_off, segment_end=end_off, error=str(e))
                    raise
                    
            if remaining != 0:
                error_msg = f"Short read segment {start_off}-{end_off}, remaining={remaining}"
                logger.error(error_msg, remote_path=remote, segment_start=start_off, 
                           segment_end=end_off, remaining=remaining)
                errors.append(RuntimeError(error_msg))
            else:
                logger.debug(f"Segment completed: {start_off}-{end_off}", 
                           remote_path=remote, segment_start=start_off, segment_end=end_off)
                           
        except Exception as e:
            logger.error(f"Segment worker error: {e}", remote_path=remote, 
                       segment_start=start_off, segment_end=end_off, error=str(e))
            errors.append(e)

    threads = [threading.Thread(target=worker, args=rng, daemon=True) for rng in ranges]
    for t in threads: t.start()
    for t in threads: t.join()

    mm.flush()
    mm.close()
    f.close()

    if errors:
        error_msg = f"FTPS segmented download failed: {errors[0]}"
        logger.error(error_msg, remote_path=remote, error_count=len(errors))
        raise RuntimeError(error_msg)

    os.replace(tmp, dst)  # atomic replace on success
    logger.debug(f"Segmented transfer completed: {rsize} bytes", 
               remote_path=remote, bytes_transferred=rsize, segments=segments)
