#!/usr/bin/env python3
"""Configuration management for rt_autodl."""

import json
from typing import Any, Dict


def load_config(path: str) -> Dict[str, Any]:
    """Load and validate configuration from JSON file (with optional comment support)."""
    with open(path, "r", encoding="utf-8") as f:
        try:
            import jsonc  # type: ignore
            cfg = jsonc.load(f)
        except ImportError:
            try:
                from json_with_comments import json as jsonc  # type: ignore
                f.seek(0)
                cfg = jsonc.load(f)
            except ImportError:
                f.seek(0)
                cfg = json.load(f)

    # Handle both legacy single label format and new multiple mappings format
    if "label_mappings" in cfg:
        # New format: multiple label mappings
        if not isinstance(cfg["label_mappings"], list) or not cfg["label_mappings"]:
            raise ValueError("config.label_mappings must be a non-empty list")
        for mapping in cfg["label_mappings"]:
            if not all(k in mapping for k in ("source", "target")):
                raise ValueError("Each label_mappings entry must have 'source' and 'target'")
    else:
        # Legacy format: single label pair
        cfg.setdefault("labels", {})
        cfg["labels"].setdefault("source", "autodl")
        cfg["labels"].setdefault("target", "downloaded")
        # Convert to new format internally
        default_dest = cfg.get("sftp", {}).get("dest_dir", "./downloads")
        cfg["label_mappings"] = [{
            "source": cfg["labels"]["source"],
            "target": cfg["labels"]["target"], 
            "dest_dir": default_dest
        }]

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
    
    # Validate configuration
    _validate_config(cfg)
    return cfg


def _validate_config(cfg: Dict[str, Any]) -> None:
    """Validate configuration values and raise meaningful errors."""
    errors = []
    
    # Validate ruTorrent URI
    uri = cfg.get("rutorrent", {}).get("uri")
    if not uri or not isinstance(uri, str):
        errors.append("rutorrent.uri is required and must be a string")
    elif not uri.startswith(('http://', 'https://')):
        errors.append("rutorrent.uri must start with http:// or https://")
    
    # Validate FTPS settings
    sftp = cfg.get("sftp", {})
    required_ftps_fields = ["ftps_host", "ftps_user"]
    for field in required_ftps_fields:
        if not sftp.get(field):
            errors.append(f"sftp.{field} is required")
    
    # Validate port
    port = sftp.get("ftps_port")
    if port is not None and (not isinstance(port, int) or port < 1 or port > 65535):
        errors.append("sftp.ftps_port must be an integer between 1 and 65535")
    
    # Validate numeric settings
    numeric_fields = {
        "ftps_timeout": (1, 300),
        "ftps_blocksize": (1024, 10 * 1024 * 1024),
        "ftps_segments": (1, 32),
        "ftps_min_seg_size": (1024, 100 * 1024 * 1024),
        "ftps_file_concurrency": (1, 16)
    }
    
    for field, (min_val, max_val) in numeric_fields.items():
        val = sftp.get(field)
        if val is not None and (not isinstance(val, int) or val < min_val or val > max_val):
            errors.append(f"sftp.{field} must be an integer between {min_val} and {max_val}")
    
    # Validate label mappings
    for i, mapping in enumerate(cfg.get("label_mappings", [])):
        if not isinstance(mapping.get("source"), str) or not mapping["source"].strip():
            errors.append(f"label_mappings[{i}].source must be a non-empty string")
        if not isinstance(mapping.get("target"), str) or not mapping["target"].strip():
            errors.append(f"label_mappings[{i}].target must be a non-empty string")
        dest_dir = mapping.get("dest_dir")
        if dest_dir is not None and (not isinstance(dest_dir, str) or not dest_dir.strip()):
            errors.append(f"label_mappings[{i}].dest_dir must be a non-empty string if provided")
    
    if errors:
        raise ValueError("Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors))