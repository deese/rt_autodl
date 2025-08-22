#!/usr/bin/env python3
"""Secrets management for rt_autodl."""

import os
from typing import Any, Dict, Optional

from rich.console import Console

try:
    from .utils import vprint
except ImportError:
    from utils import vprint


def maybe_load_dotenv(cfg: Dict[str, Any], console: Console) -> None:
    """Load environment variables from .env file if configured."""
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
    """Expand environment variables in string."""
    if not isinstance(s, str):
        return s
    try:
        return os.path.expandvars(s)
    except Exception:
        return s


def resolve_secret(val: Optional[str], cfg: Dict[str, Any], *, username: Optional[str] = None, console: Optional[Console] = None) -> Optional[str]:
    """Resolve secret from various sources (env, dotenv, keyring)."""
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