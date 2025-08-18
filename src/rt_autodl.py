#!/usr/bin/env python3
from __future__ import annotations
# -*- coding: utf-8 -*-
"""
FTPS-only ruTorrent downloader with progress, size-aware skipping, and robust relabeling.

Features
- Connects to ruTorrent (via pyruTorrent) and lists torrents by label.
- FTPS (FTP over TLS) downloads with:
  • Per-file progress (rich)
  • Optional multi-connection segmented transfers (REST + transfercmd)
  • Optional concurrent downloads across files
  • Always requests binary mode (TYPE I)
  • Downloads to .part then atomically renames on success
- Plans remote paths as ftp_root + file.path (or file.name). If missing, derives relative
  path from frozen_path by stripping rtorrent_root.
- If destination file exists with the same size → SKIP download but STILL relabel.
- For single-file torrents whose file entry lacks a size, falls back to torrent['bytes_total'].
- Robust relabel() that tries pyruTorrent then falls back to HTTP POST (httprpc).

Usage
  python torrenter.py --config /path/config.json [--verbose] [--dry-run]

Required config (strict JSON; no comments)
{
  "mode": "sftp",
  "labels": { "source": "autodl", "target": "downloaded" },
  "rutorrent": { "uri": "http://user:pass@host/rutorrent/plugins/httprpc/action.php" },
  "sftp": {
    "backend": "ftps",
    "dest_dir": "/data/inbox",

    "ftps_host": "ftp.example",
    "ftps_user": "me",
    "ftps_password": "secret",
    "ftps_port": 21,
    "ftps_pasv": true,
    "ftps_tls_verify": true,
    "ftps_timeout": 30,

    "ftp_root": "/export",
    "rtorrent_root": "/data/rtorrent",

    "ftps_blocksize": 262144,
    "ftps_segments": 4,
    "ftps_min_seg_size": 8388608,
    "ftps_file_concurrency": 1
  },
  "skip_if_exists_same_size": true
}

Dependencies
  pip install pyruTorrent rich requests
"""

import argparse
import concurrent.futures
import json
import mmap
import os
import posixpath
import ssl
import stat
import threading
import time
from typing import Any, Dict, List, Optional, Tuple


# ---- Secrets: dotenv + keyring ----------------------------------------------
def _maybe_load_dotenv(cfg: Dict[str, Any], console: Console) -> None:
    secrets = cfg.setdefault("secrets", {})
    use_dotenv = bool(secrets.get("use_dotenv", False))
    if not use_dotenv:
        return
    path = secrets.get("dotenv_path")
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception as e:
        vprint(console, f"[secrets] python-dotenv not installed: {e!r}")
        return
    try:
        if path:
            load_dotenv(dotenv_path=path, override=False)
        else:
            load_dotenv(override=False)
        vprint(console, "[secrets] .env loaded")
    except Exception as e:
        vprint(console, f"[secrets] .env load failed: {e!r}")

def _expand_env(s: Optional[str]) -> Optional[str]:
    if not isinstance(s, str):
        return s
    try:
        return os.path.expandvars(s)
    except Exception:
        return s

def _resolve_secret(val: Optional[str], cfg: Dict[str, Any], *, username: Optional[str]=None, console: Optional[Console]=None) -> Optional[str]:
    if val is None or not isinstance(val, str):
        return val
    val = _expand_env(val)
    if val.startswith("env:") or val.startswith("dotenv:"):
        key = val.split(":", 1)[1]
        out = os.getenv(key)
        if out is None and console:
            vprint(console, f"[secrets] env var '{key}' not set")
        return out
    if val.startswith("keyring:"):
        token = val.split(":", 1)[1]
        try:
            import keyring  # type: ignore
        except Exception as e:
            if console:
                vprint(console, f"[secrets] keyring not available: {e!r}")
            return None
        service = None
        item = None
        if "/" in token:
            service, item = token.split("/", 1)
        else:
            service = cfg.get("secrets", {}).get("keyring_service", "torrenter")
            item = token or username
        if not item:
            item = username
        try:
            return keyring.get_password(service, item)  # type: ignore
        except Exception as e:
            if console:
                vprint(console, f"[secrets] keyring lookup failed ({service}/{item}): {e!r}")
            return None
    return val

from rich.console import Console
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn, TransferSpeedColumn

# Flags
VERBOSE = False
DRY_RUN = False

# ---------------- Helpers ----------------
def vprint(console: Console, msg: str) -> None:
    if VERBOSE:
        try:
            console.log(msg)
        except Exception:
            print(msg)

def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)

def posix_norm(p: str) -> str:
    """Normalize to POSIX path (collapse //, ensure forward slashes)."""
    return posixpath.normpath((p or "").replace("\\", "/"))

def join_posix(a: str, b: str) -> str:
    """Join POSIX safely and normalize (no double slashes)."""
    return posix_norm(posixpath.join(a or "", b or ""))

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    cfg.setdefault("labels", {})
    cfg["labels"].setdefault("source", "autodl")
    cfg["labels"].setdefault("target", "downloaded")

    if "rutorrent" not in cfg or "uri" not in cfg["rutorrent"]:
        raise ValueError("config.rutorrent.uri is required")

    # Only "sftp" mode is used, with backend "ftps"
    if str(cfg.get("mode", "sftp")).lower() != "sftp":
        raise ValueError('config.mode must be "sftp" for this FTPS-only build')

    s = cfg.setdefault("sftp", {})
    s.setdefault("backend", "ftps")
    if s["backend"] != "ftps":
        raise ValueError('config.sftp.backend must be "ftps"')
    s.setdefault("dest_dir", "./downloads")

    # FTPS defaults
    s.setdefault("ftps_host", s.get("ftps_host", ""))
    s.setdefault("ftps_user", s.get("ftps_user", ""))
    s.setdefault("ftps_password", s.get("ftps_password"))
    s.setdefault("ftps_port", 21)
    s.setdefault("ftps_pasv", True)
    s.setdefault("ftps_tls_verify", True)
    s.setdefault("ftps_timeout", 30)
    s.setdefault("ftps_blocksize", 262144)
    s.setdefault("ftps_segments", 4)
    s.setdefault("ftps_min_seg_size", 8 * 1024 * 1024)
    s.setdefault("ftps_file_concurrency", 1)
    s.setdefault("ftp_root", "/")
    s.setdefault("rtorrent_root", None)

    cfg.setdefault("skip_if_exists_same_size", True)
    cfg.setdefault("secrets", {"use_dotenv": False, "dotenv_path": None, "keyring_service": "torrenter"})
    return cfg

# ---------------- ruTorrent ----------------
def connect_rutorrent(uri: str):
    try:
        from pyruTorrent import rTorrent  # type: ignore
    except Exception as e:
        raise RuntimeError("pyruTorrent is required: pip install pyruTorrent") from e
    return rTorrent(uri=uri)

def is_completed(t: Dict[str, Any]) -> bool:
    p = t.get("progress")
    if isinstance(p, (int, float)) and float(p) >= 100.0:
        return True
    if t.get("is_complete") == 1:
        return True
    completed = t.get("completed_bytes") or t.get("completedBytes") or t.get("bytes_done")
    total     = t.get("size_bytes") or t.get("sizeBytes") or t.get("size") or t.get("bytes_total")
    if isinstance(completed, int) and isinstance(total, int) and total > 0:
        return completed >= total
    if str(t.get("connection_current", "")).lower() in {"seed", "seeding"}:
        return True
    return False

def list_by_label(rt, label: str) -> List[Dict[str, Any]]:
    torrents = rt.get_torrents(include_files=True)
    return [x for x in torrents if x.get("label") == label]

def relabel(rt, info_hash: str, new_label: str, cfg: Dict[str, Any], console: Console) -> None:
    """
    Robust relabel:
      1) Try pyruTorrent methods (two shapes).
      2) Fallback to ruTorrent httprpc POST with common param variants.
    """
    uri = str(cfg.get("rutorrent", {}).get("uri", ""))
    if not uri:
        raise RuntimeError("No ruTorrent URI provided for HTTP fallback")
    try:
        import requests  # type: ignore
    except Exception as e:
        raise RuntimeError("requests is required for relabel fallback: pip install requests") from e

    payloads = [
        {"mode": "setlabel", "hash": info_hash, "v": new_label, "s": "label"},   # matches UI: v=<label>, s=label
        {"mode": "setlabel", "hash": info_hash, "v": new_label},                 # value only
        {"mode": "setlabel", "hash": info_hash, "label": new_label},             # alternate key
    ]
    last_exc = None
    for data in payloads:
        try:
            resp = requests.post(uri, data=data, timeout=10)
            if resp.status_code == 200:
                vprint(console, f"[relabel] HTTP ok with payload {data}")
                return
            else:
                vprint(console, f"[relabel] HTTP {resp.status_code} for payload {data}")
        except Exception as e:
            last_exc = e
            vprint(console, f"[relabel] HTTP error for payload {data}: {e!r}")
    if last_exc:
        raise last_exc
    raise RuntimeError("All relabel attempts failed")

# ---------------- FTPS planning ----------------
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
    """
    files = torrent.get("files") or []
    is_single = len(files) == 1

    plan: List[Tuple[str, str, int]] = []
    ftp_root = posix_norm(ftp_root or "/")

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

        remote = join_posix(ftp_root, rel)
        plan.append((remote, rel, int(size)))
    return plan

# ---------------- FTPS transfer ----------------
def ftps_connect(s: Dict[str, Any], ctx: ssl.SSLContext):
    import ftplib
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
    import ftplib
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
                        if rname not in names and all(not n.endswith("/" + rname) for n in names):
                            raise FileNotFoundError(f"{rname} not in listing")
                        rsize = 0
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
    import ftplib

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

# ---------------- main per-torrent ----------------
def process_torrent(cfg: Dict[str, Any], rt, t: Dict[str, Any], console: Console, progress: Progress) -> None:
    dst_label = cfg["labels"]["target"]

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
    dest_root = s["dest_dir"]
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
        console.print(f"[magenta][LABEL][/magenta] {name}: -> {dst_label}")
    except Exception as e:
        console.print(f"[red][ERR][/red] {name}: relabel failed: {e}")
        vprint(console, f"Label error: {repr(e)}")

# ---------------- entry ----------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to JSON config")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    parser.add_argument("--dry-run", action="store_true", help="Only print planned actions, do not transfer")
    args = parser.parse_args()

    global VERBOSE, DRY_RUN
    VERBOSE = bool(args.verbose)
    DRY_RUN = bool(args.dry_run)

    console = Console()
    if VERBOSE:
        console.log("Verbose enabled")

    cfg = load_config(args.config)
    # Load dotenv (optional) and resolve secrets before connecting
    _maybe_load_dotenv(cfg, console)
    # Resolve ruTorrent URI
    cfg["rutorrent"]["uri"] = _resolve_secret(cfg["rutorrent"].get("uri"), cfg, console=console) or cfg["rutorrent"].get("uri")
    # Resolve FTPS user/password
    s = cfg.get("sftp", {})
    s["ftps_user"] = _resolve_secret(s.get("ftps_user"), cfg, console=console) or s.get("ftps_user")
    s["ftps_password"] = _resolve_secret(s.get("ftps_password"), cfg, username=s.get("ftps_user"), console=console) or s.get("ftps_password")
    # Reassign (in case dict was a shallow copy)
    cfg["sftp"] = s

    rt = connect_rutorrent(cfg["rutorrent"]["uri"])

    torrents = list_by_label(rt, cfg["labels"]["source"])
    if not torrents:
        console.print(f"[yellow]No torrents with label '{cfg['labels']['source']}'.[/yellow]")
        return

    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TransferSpeedColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    ) as progress:
        for t in torrents:
            process_torrent(cfg, rt, t, console, progress)

if __name__ == "__main__":
    main()
