#!/usr/bin/env python3
"""Utility functions for rt_autodl."""

import os
import posixpath
import time
from functools import wraps

from rich.console import Console


# Flags
VERBOSE = False
DRY_RUN = False


def set_flags(verbose: bool = False, dry_run: bool = False) -> None:
    """Set global flags."""
    global VERBOSE, DRY_RUN
    VERBOSE = verbose
    DRY_RUN = dry_run


def retry_on_failure(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator for retrying failed operations with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:  # Don't sleep on the last attempt
                        if VERBOSE:
                            console = Console()
                            vprint(console, f"[retry] Attempt {attempt + 1} failed: {e!r}, retrying in {current_delay}s")
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        if VERBOSE:
                            console = Console()
                            vprint(console, f"[retry] All {max_attempts} attempts failed")
            
            raise last_exception or RuntimeError(f"Function {func.__name__} failed after {max_attempts} attempts")
        return wrapper
    return decorator


def vprint(console: Console, msg: str) -> None:
    """Print verbose message if verbose mode is enabled."""
    if VERBOSE:
        try:
            console.log(msg)
        except Exception:
            print(msg)


def ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    if path:
        os.makedirs(path, exist_ok=True)


def posix_norm(p: str) -> str:
    """Normalize to POSIX path (collapse //, ensure forward slashes)."""
    return posixpath.normpath((p or "").replace("\\", "/"))


def join_posix(a: str, b: str) -> str:
    """Join POSIX safely and normalize (no double slashes)."""
    return posix_norm(posixpath.join(a or "", b or ""))