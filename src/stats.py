#!/usr/bin/env python3
"""Statistics tracking system for rt_autodl."""

import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, List, Optional
from datetime import datetime, timedelta


@dataclass
class TransferStats:
    """Statistics for a single file transfer."""
    file_path: str
    torrent_hash: str
    start_time: float
    end_time: Optional[float] = None
    bytes_transferred: int = 0
    total_size: int = 0
    error: Optional[str] = None
    
    @property
    def duration(self) -> float:
        """Get transfer duration in seconds."""
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time
    
    @property
    def average_speed(self) -> float:
        """Get average transfer speed in bytes/second."""
        duration = self.duration
        if duration <= 0:
            return 0
        return self.bytes_transferred / duration
    
    @property
    def is_complete(self) -> bool:
        """Check if transfer is complete."""
        return self.end_time is not None
    
    @property
    def is_successful(self) -> bool:
        """Check if transfer was successful."""
        return self.is_complete and self.error is None


@dataclass
class SessionStats:
    """Statistics for the entire session."""
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    total_torrents_processed: int = 0
    total_files_attempted: int = 0
    total_files_successful: int = 0
    total_files_failed: int = 0
    total_bytes_transferred: int = 0
    total_bytes_skipped: int = 0
    
    # Transfer statistics
    transfers: List[TransferStats] = field(default_factory=list)
    
    # Connection statistics
    connection_attempts: int = 0
    connection_failures: int = 0
    connection_pool_hits: int = 0
    connection_pool_misses: int = 0
    
    @property
    def duration(self) -> float:
        """Get session duration in seconds."""
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time
    
    @property
    def average_transfer_speed(self) -> float:
        """Get average transfer speed across all transfers in bytes/second."""
        duration = self.duration
        if duration <= 0:
            return 0
        return self.total_bytes_transferred / duration
    
    @property
    def success_rate(self) -> float:
        """Get success rate as percentage."""
        if self.total_files_attempted == 0:
            return 0
        return (self.total_files_successful / self.total_files_attempted) * 100
    
    @property
    def connection_success_rate(self) -> float:
        """Get connection success rate as percentage."""
        if self.connection_attempts == 0:
            return 0
        return ((self.connection_attempts - self.connection_failures) / self.connection_attempts) * 100
    
    @property
    def connection_pool_hit_rate(self) -> float:
        """Get connection pool hit rate as percentage."""
        total_requests = self.connection_pool_hits + self.connection_pool_misses
        if total_requests == 0:
            return 0
        return (self.connection_pool_hits / total_requests) * 100


class StatsTracker:
    """Thread-safe statistics tracker for rt_autodl operations."""
    
    def __init__(self):
        self._lock = Lock()
        self.session = SessionStats()
        self._active_transfers: Dict[str, TransferStats] = {}
    
    def start_session(self):
        """Start a new session."""
        with self._lock:
            self.session = SessionStats()
    
    def end_session(self):
        """End the current session."""
        with self._lock:
            self.session.end_time = time.time()
    
    def start_transfer(self, file_path: str, torrent_hash: str, total_size: int = 0) -> str:
        """Start tracking a new transfer. Returns transfer ID."""
        transfer_id = f"{torrent_hash}:{file_path}"
        with self._lock:
            transfer = TransferStats(
                file_path=file_path,
                torrent_hash=torrent_hash,
                start_time=time.time(),
                total_size=total_size
            )
            self._active_transfers[transfer_id] = transfer
            self.session.total_files_attempted += 1
        return transfer_id
    
    def update_transfer_progress(self, transfer_id: str, bytes_transferred: int):
        """Update transfer progress."""
        with self._lock:
            if transfer_id in self._active_transfers:
                self._active_transfers[transfer_id].bytes_transferred = bytes_transferred
    
    def complete_transfer(self, transfer_id: str, success: bool = True, error: Optional[str] = None):
        """Mark transfer as complete."""
        with self._lock:
            if transfer_id in self._active_transfers:
                transfer = self._active_transfers[transfer_id]
                transfer.end_time = time.time()
                transfer.error = error
                
                # Update session stats
                if success and error is None:
                    self.session.total_files_successful += 1
                    self.session.total_bytes_transferred += transfer.bytes_transferred
                else:
                    self.session.total_files_failed += 1
                
                # Move to completed transfers
                self.session.transfers.append(transfer)
                del self._active_transfers[transfer_id]
    
    def skip_transfer(self, file_path: str, torrent_hash: str, size: int = 0, reason: str = "already_exists"):
        """Record a skipped transfer."""
        with self._lock:
            self.session.total_bytes_skipped += size
    
    def record_torrent_processed(self):
        """Record that a torrent was processed."""
        with self._lock:
            self.session.total_torrents_processed += 1
    
    def record_connection_attempt(self, success: bool = True):
        """Record a connection attempt."""
        with self._lock:
            self.session.connection_attempts += 1
            if not success:
                self.session.connection_failures += 1
    
    def record_connection_pool_hit(self):
        """Record a connection pool hit."""
        with self._lock:
            self.session.connection_pool_hits += 1
    
    def record_connection_pool_miss(self):
        """Record a connection pool miss."""
        with self._lock:
            self.session.connection_pool_misses += 1
    
    def get_active_transfers(self) -> List[TransferStats]:
        """Get list of currently active transfers."""
        with self._lock:
            return list(self._active_transfers.values())
    
    def get_session_summary(self) -> Dict:
        """Get a summary of the session statistics."""
        with self._lock:
            return {
                "duration": self.session.duration,
                "torrents_processed": self.session.total_torrents_processed,
                "files": {
                    "attempted": self.session.total_files_attempted,
                    "successful": self.session.total_files_successful,
                    "failed": self.session.total_files_failed,
                    "success_rate": round(self.session.success_rate, 2)
                },
                "bytes": {
                    "transferred": self.session.total_bytes_transferred,
                    "skipped": self.session.total_bytes_skipped,
                    "total": self.session.total_bytes_transferred + self.session.total_bytes_skipped
                },
                "transfer_speed": {
                    "average": round(self.session.average_transfer_speed, 2),
                    "average_mbps": round(self.session.average_transfer_speed / (1024 * 1024), 2)
                },
                "connections": {
                    "attempts": self.session.connection_attempts,
                    "failures": self.session.connection_failures,
                    "success_rate": round(self.session.connection_success_rate, 2),
                    "pool_hit_rate": round(self.session.connection_pool_hit_rate, 2)
                }
            }
    
    def get_transfer_history(self, limit: int = 100) -> List[Dict]:
        """Get transfer history with details."""
        with self._lock:
            transfers = sorted(self.session.transfers, key=lambda t: t.start_time, reverse=True)[:limit]
            return [
                {
                    "file_path": t.file_path,
                    "torrent_hash": t.torrent_hash,
                    "start_time": datetime.fromtimestamp(t.start_time).isoformat(),
                    "duration": round(t.duration, 2),
                    "bytes_transferred": t.bytes_transferred,
                    "total_size": t.total_size,
                    "average_speed": round(t.average_speed, 2),
                    "average_speed_mbps": round(t.average_speed / (1024 * 1024), 2),
                    "successful": t.is_successful,
                    "error": t.error
                }
                for t in transfers
            ]
    
    def format_speed(self, bytes_per_second: float) -> str:
        """Format transfer speed in human-readable format."""
        if bytes_per_second < 1024:
            return f"{bytes_per_second:.1f} B/s"
        elif bytes_per_second < 1024 * 1024:
            return f"{bytes_per_second / 1024:.1f} KB/s"
        elif bytes_per_second < 1024 * 1024 * 1024:
            return f"{bytes_per_second / (1024 * 1024):.1f} MB/s"
        else:
            return f"{bytes_per_second / (1024 * 1024 * 1024):.1f} GB/s"
    
    def format_size(self, bytes_size: int) -> str:
        """Format file size in human-readable format."""
        if bytes_size < 1024:
            return f"{bytes_size} B"
        elif bytes_size < 1024 * 1024:
            return f"{bytes_size / 1024:.1f} KB"
        elif bytes_size < 1024 * 1024 * 1024:
            return f"{bytes_size / (1024 * 1024):.1f} MB"
        else:
            return f"{bytes_size / (1024 * 1024 * 1024):.1f} GB"


# Global stats tracker
_stats_tracker: Optional[StatsTracker] = None


def get_stats_tracker() -> StatsTracker:
    """Get the global statistics tracker."""
    global _stats_tracker
    if _stats_tracker is None:
        _stats_tracker = StatsTracker()
    return _stats_tracker


def reset_stats():
    """Reset the global statistics tracker."""
    global _stats_tracker
    _stats_tracker = StatsTracker()