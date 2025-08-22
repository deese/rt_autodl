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
  python rt_autodl.py --config /path/config.json [--verbose] [--dry-run]

Required config (supports JSON with comments)
{
  // Single label pair (legacy format)
  "labels": { "source": "autodl", "target": "downloaded" },
  // OR multiple label pairs (new format)
  "label_mappings": [
    { "source": "autodl", "target": "downloaded", "dest_dir": "/data/inbox" },
    { "source": "movies", "target": "processed", "dest_dir": "/data/movies" }
  ],
  
  "rutorrent": { "uri": "http://user:pass@host/rutorrent/plugins/httprpc/action.php" },
  "sftp": {
    "backend": "ftps",
    "dest_dir": "/data/inbox", // default if not specified in label_mappings

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

# Import from the new modular structure
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from main import main

if __name__ == "__main__":
    main()
