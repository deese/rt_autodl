#!/usr/bin/env python3
"""ruTorrent client functionality for rt_autodl."""

from functools import wraps
from typing import Any, Dict, List

from rich.console import Console

try:
    from .utils import retry_on_failure, vprint
except ImportError:
    from utils import retry_on_failure, vprint


@retry_on_failure(max_attempts=3, delay=1.0)
def connect_rutorrent(uri: str):
    """Connect to ruTorrent via pyruTorrent."""
    try:
        from pyruTorrent import rTorrent  # type: ignore
    except Exception as e:
        raise RuntimeError("pyruTorrent is required: pip install pyruTorrent") from e
    return rTorrent(uri=uri)


def is_completed(t: Dict[str, Any]) -> bool:
    """Check if torrent is completed."""
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
    """Get torrents with specific label."""
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