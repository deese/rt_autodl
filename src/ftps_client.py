#!/usr/bin/env python3
"""FTPS client functionality for rt_autodl."""

import concurrent.futures
import ftplib
import mmap
import os
import posixpath
import ssl
import threading
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.progress import Progress

try:
    from .utils import retry_on_failure, vprint, ensure_dir, posix_norm, join_posix
except ImportError:
    from utils import retry_on_failure, vprint, ensure_dir, posix_norm, join_posix


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
            # Extract just the filename from the rel path for multi-file torrents
            filename = posixpath.basename(rel)
            remote_rel = join_posix(torrent_name, filename)
        else:
            remote_rel = rel
            
        remote = join_posix(ftp_root, remote_rel)
        plan.append((remote, rel, int(size)))
    return plan


@retry_on_failure(max_attempts=3, delay=2.0)
def ftps_connect(s: Dict[str, Any], ctx: ssl.SSLContext):
    """Establish FTPS connection."""
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


def _ftps_resolve_remote(s: Dict[str, Any], ctx: ssl.SSLContext, remote: str) -> Tuple[str, str, str, int]:
    """
    Try multiple candidate paths by progressively stripping leading components from the
    relative portion under ftp_root until we find one that exists. Returns (canonical_remote, rdir, rname, size).
    """
    ftp = ftps_connect(s, ctx)
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
                        names = ftp.nlst()
                        # Try exact match first
                        if rname in names:
                            rsize = 0
                        else:
                            # Try case-insensitive match
                            rname_lower = rname.lower()
                            found = False
                            for n in names:
                                if n.lower() == rname_lower:
                                    rname = n  # Use the actual filename from server
                                    rsize = 0
                                    found = True
                                    break
                                elif n.endswith("/" + rname):
                                    rname = n.split("/")[-1]  # Extract filename
                                    rsize = 0
                                    found = True
                                    break
                                elif n.lower().endswith("/" + rname_lower):
                                    rname = n.split("/")[-1]  # Extract filename
                                    rsize = 0
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
    finally:
        try:
            ftp.quit()
        except Exception:
            try: ftp.close()
            except Exception: pass


def ftps_get(cfg: Dict[str, Any], remote: str, dst: str, size_hint: int, progress: Progress, task_id=None) -> None:
    """
    FTPS download with optional multi-connection segmentation using REST + transfercmd.
    If the destination exists with same size, SKIP download (but caller still relabels).
    """
    s = cfg["sftp"]
    ensure_dir(os.path.dirname(dst))
    remote = posix_norm(remote)

    # TLS context for resolution/probe
    ctx_probe = ssl.create_default_context()
    if not s.get("ftps_tls_verify", True):
        ctx_probe.check_hostname = False
        ctx_probe.verify_mode = ssl.CERT_NONE

    # Resolve canonical path & probed size
    canonical_remote, rdir, rname, probed_size = _ftps_resolve_remote(s, ctx_probe, remote)
    remote = canonical_remote

    # Fast skip if same-size destination exists
    if os.path.exists(dst):
        try:
            if probed_size > 0 and os.path.getsize(dst) == probed_size:
                vprint(Console(), f"[FTPS] skip (exists same size): {dst}")
                progress.update(task_id, completed=probed_size) if task_id is not None else None
                return
            if size_hint > 0 and os.path.getsize(dst) == size_hint:
                vprint(Console(), f"[FTPS] skip (exists same size, hint): {dst}")
                progress.update(task_id, completed=size_hint) if task_id is not None else None
                return
        except Exception:
            pass

    # TLS context for transfers
    ctx = ssl.create_default_context()
    if not s.get("ftps_tls_verify", True):
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    blocksize = int(s.get("ftps_blocksize", 262144))
    segments  = int(s.get("ftps_segments", 1))
    min_seg   = int(s.get("ftps_min_seg_size", 8 * 1024 * 1024))

    # Determine size for segmentation decision
    rsize = probed_size if probed_size > 0 else size_hint
    if rsize <= 0:
        ftps0 = ftps_connect(s, ctx)
        try:
            if rdir and rdir not in {"", "/"}:
                ftps0.cwd(rdir)
            try:
                rsize = ftps0.size(rname) or 0
            except Exception:
                rsize = 0
        finally:
            try:
                ftps0.quit()
            except Exception:
                try: ftps0.close()
                except Exception: pass

    # Single-stream path
    if rsize <= 0 or segments <= 1 or rsize < max(min_seg, segments * blocksize):
        ftps = ftps_connect(s, ctx)
        try:
            if rdir and rdir not in {"", "/"}:
                ftps.cwd(rdir)
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
                ftps.retrbinary(f"RETR {rname}", _cb, blocksize=blocksize)
            os.replace(tmp, dst)  # atomic replace on success
        finally:
            try:
                ftps.quit()
            except Exception:
                try: ftps.close()
                except Exception: pass
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
    for i in range(segments):
        start_off = i * seg_size
        end_off = rsize if i == segments - 1 else (i + 1) * seg_size
        ranges.append((start_off, end_off))

    errors: List[Exception] = []

    def worker(start_off: int, end_off: int):
        try:
            ftp = ftps_connect(s, ctx)
            try:
                if rdir and rdir not in {"", "/"}:
                    ftp.cwd(rdir)
                datasock = ftp.transfercmd(f"RETR {rname}", rest=start_off)
                remaining = end_off - start_off
                pos = start_off
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
            finally:
                try: ftp.quit()
                except Exception:
                    try: ftp.close()
                    except Exception: pass
            if remaining != 0:
                errors.append(RuntimeError(f"Short read segment {start_off}-{end_off}, remaining={remaining}"))
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=rng, daemon=True) for rng in ranges]
    for t in threads: t.start()
    for t in threads: t.join()

    mm.flush()
    mm.close()
    f.close()

    if errors:
        raise RuntimeError(f"FTPS segmented download failed: {errors[0]}")

    os.replace(tmp, dst)  # atomic replace on success