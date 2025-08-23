#!/usr/bin/env python3
"""Structured logging system for rt_autodl."""

import json
import logging
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional, Union
from contextlib import contextmanager

from rich.console import Console
from rich.logging import RichHandler


class StructuredLogger:
    """Structured logger with JSON and console output capabilities."""
    
    def __init__(self, name: str = "rt_autodl", level: str = "INFO", 
                 json_output: bool = False, console_output: bool = True):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        self.logger.handlers.clear()
        
        self.json_output = json_output
        self.console_output = console_output
        
        # JSON formatter for structured logs
        if json_output:
            json_handler = logging.StreamHandler(sys.stdout)
            json_handler.setFormatter(JSONFormatter())
            self.logger.addHandler(json_handler)
        
        # Rich console handler for human-readable output
        if console_output:
            self.console = Console()
            rich_handler = RichHandler(console=self.console, show_time=True, show_path=False)
            rich_handler.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(rich_handler)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with optional structured data."""
        self._log(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message with optional structured data."""
        self._log(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with optional structured data."""
        self._log(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with optional structured data."""
        self._log(logging.ERROR, message, **kwargs)
    
    def _log(self, level: int, message: str, **kwargs):
        """Internal logging method that handles structured data."""
        if kwargs:
            extra = {"structured_data": kwargs}
        else:
            extra = {}
        self.logger.log(level, message, extra=extra)
    
    def transfer_start(self, torrent_name: str, torrent_hash: str, 
                      file_count: int, total_size: int, destination: str):
        """Log start of transfer with structured data."""
        self.info(
            f"Starting transfer: {torrent_name}",
            event="transfer_start",
            torrent_name=torrent_name,
            torrent_hash=torrent_hash,
            file_count=file_count,
            total_size=total_size,
            destination=destination
        )
    
    def transfer_progress(self, torrent_hash: str, file_path: str, 
                         bytes_transferred: int, total_size: int, 
                         transfer_speed: float = 0):
        """Log transfer progress with structured data."""
        self.debug(
            f"Transfer progress: {file_path}",
            event="transfer_progress",
            torrent_hash=torrent_hash,
            file_path=file_path,
            bytes_transferred=bytes_transferred,
            total_size=total_size,
            transfer_speed=transfer_speed,
            progress_percent=round((bytes_transferred / total_size) * 100, 2) if total_size > 0 else 0
        )
    
    def transfer_complete(self, torrent_hash: str, file_path: str, 
                         bytes_transferred: int, duration: float):
        """Log transfer completion with structured data."""
        self.info(
            f"Transfer complete: {file_path}",
            event="transfer_complete",
            torrent_hash=torrent_hash,
            file_path=file_path,
            bytes_transferred=bytes_transferred,
            duration=duration,
            average_speed=bytes_transferred / duration if duration > 0 else 0
        )
    
    def transfer_error(self, torrent_hash: str, file_path: str, error: str):
        """Log transfer error with structured data."""
        self.error(
            f"Transfer failed: {file_path} - {error}",
            event="transfer_error",
            torrent_hash=torrent_hash,
            file_path=file_path,
            error=error
        )
    
    def relabel_success(self, torrent_hash: str, torrent_name: str, 
                       from_label: str, to_label: str):
        """Log successful relabeling with structured data."""
        self.info(
            f"Relabeled torrent: {torrent_name} ({from_label} → {to_label})",
            event="relabel_success",
            torrent_hash=torrent_hash,
            torrent_name=torrent_name,
            from_label=from_label,
            to_label=to_label
        )
    
    def relabel_error(self, torrent_hash: str, torrent_name: str, 
                     from_label: str, to_label: str, error: str):
        """Log relabeling error with structured data."""
        self.error(
            f"Relabel failed: {torrent_name} ({from_label} → {to_label}) - {error}",
            event="relabel_error",
            torrent_hash=torrent_hash,
            torrent_name=torrent_name,
            from_label=from_label,
            to_label=to_label,
            error=error
        )
    
    @contextmanager
    def operation_timer(self, operation: str, **context):
        """Context manager for timing operations."""
        start_time = time.time()
        self.debug(f"Starting operation: {operation}", event="operation_start", 
                  operation=operation, **context)
        try:
            yield
            duration = time.time() - start_time
            self.debug(f"Completed operation: {operation}", event="operation_complete",
                      operation=operation, duration=duration, **context)
        except Exception as e:
            duration = time.time() - start_time
            self.error(f"Failed operation: {operation} - {e}", event="operation_error",
                      operation=operation, duration=duration, error=str(e), **context)
            raise


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage()
        }
        
        # Add structured data if present
        if hasattr(record, "structured_data"):
            log_entry.update(record.structured_data)
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, ensure_ascii=False)


# Global logger instance
_logger: Optional[StructuredLogger] = None


def get_logger() -> StructuredLogger:
    """Get the global logger instance."""
    global _logger
    if _logger is None:
        _logger = StructuredLogger()
    return _logger


def init_logger(level: str = "INFO", json_output: bool = False, 
               console_output: bool = True) -> StructuredLogger:
    """Initialize the global logger with specified settings."""
    global _logger
    _logger = StructuredLogger(level=level, json_output=json_output, 
                              console_output=console_output)
    return _logger


def log_config_loaded(config_path: str, label_count: int):
    """Log configuration loading."""
    logger = get_logger()
    logger.info(
        f"Configuration loaded: {config_path}",
        event="config_loaded",
        config_path=config_path,
        label_mappings_count=label_count
    )


def log_session_start():
    """Log session start."""
    logger = get_logger()
    logger.info(
        "RT AutoDL session started",
        event="session_start",
        timestamp=datetime.now().isoformat()
    )


def log_session_end(total_torrents: int = 0, success_count: int = 0, 
                   error_count: int = 0):
    """Log session end with summary."""
    logger = get_logger()
    logger.info(
        f"RT AutoDL session ended - {success_count}/{total_torrents} successful",
        event="session_end",
        total_torrents=total_torrents,
        success_count=success_count,
        error_count=error_count,
        timestamp=datetime.now().isoformat()
    )