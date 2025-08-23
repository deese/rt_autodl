#!/usr/bin/env python3
"""FTPS connection pooling for rt_autodl."""

import ftplib
import ssl
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from queue import Queue, Empty
from typing import Dict, Any, Optional, Generator

try:
    from .stats import get_stats_tracker
    from .logger import get_logger
except ImportError:
    from stats import get_stats_tracker
    from logger import get_logger


@dataclass
class PooledConnection:
    """A pooled FTPS connection with metadata."""
    connection: ftplib.FTP_TLS
    created_at: float
    last_used: float
    use_count: int = 0
    
    def is_expired(self, max_age: float) -> bool:
        """Check if connection has expired."""
        return time.time() - self.created_at > max_age
    
    def is_idle_too_long(self, max_idle: float) -> bool:
        """Check if connection has been idle too long."""
        return time.time() - self.last_used > max_idle
    
    def mark_used(self):
        """Mark connection as recently used."""
        self.last_used = time.time()
        self.use_count += 1


class FTPSConnectionPool:
    """Thread-safe FTPS connection pool."""
    
    def __init__(self, config: Dict[str, Any], max_connections: int = 8, 
                 max_connection_age: float = 300, max_idle_time: float = 60):
        self.config = config
        self.max_connections = max_connections
        self.max_connection_age = max_connection_age
        self.max_idle_time = max_idle_time
        
        self._pool: Queue[PooledConnection] = Queue(maxsize=max_connections)
        self._lock = threading.Lock()
        self._active_connections = 0
        self._stats = get_stats_tracker()
        self._logger = get_logger()
        
        # SSL context (reused for all connections)
        self._ssl_context = self._create_ssl_context()
        
        # Background cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_expired, daemon=True)
        self._cleanup_running = True
        self._cleanup_thread.start()
    
    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context for FTPS connections."""
        ctx = ssl.create_default_context()
        if not self.config.get("ftps_tls_verify", True):
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx
    
    def _create_connection(self) -> ftplib.FTP_TLS:
        """Create a new FTPS connection."""
        s = self.config
        timeout = int(s.get("ftps_timeout", 30))
        pasv = bool(s.get("ftps_pasv", True))
        
        try:
            ftp = ftplib.FTP_TLS(timeout=timeout, context=self._ssl_context)
        except TypeError:
            # Fallback for older Python versions
            ftp = ftplib.FTP_TLS(timeout=timeout)
            ftp.context = self._ssl_context  # type: ignore[attr-defined]
        
        try:
            self._logger.debug(f"Creating FTPS connection to {s['ftps_host']}:{s.get('ftps_port', 21)}")
            ftp.connect(s["ftps_host"], int(s.get("ftps_port", 21)))
            ftp.login(user=s["ftps_user"], passwd=(s.get("ftps_password") or ""))
            ftp.prot_p()
            ftp.set_pasv(pasv)
            
            # Ensure binary mode - critical for resuming transfers
            try:
                ftp.voidcmd('TYPE I')
                self._logger.debug("Set FTPS connection to binary mode")
            except Exception as e:
                self._logger.warning(f"Failed to set binary mode, may cause resume issues: {e}")
                # Still proceed as some servers might default to binary
            
            self._stats.record_connection_attempt(success=True)
            return ftp
            
        except Exception as e:
            self._stats.record_connection_attempt(success=False)
            self._logger.error(f"Failed to create FTPS connection: {e}")
            try:
                ftp.close()
            except Exception:
                pass
            raise
    
    @contextmanager
    def get_connection(self) -> Generator[ftplib.FTP_TLS, None, None]:
        """Get a connection from the pool (context manager)."""
        connection = None
        pooled_conn = None
        
        try:
            # Try to get from pool first
            try:
                pooled_conn = self._pool.get_nowait()
                connection = pooled_conn.connection
                
                # Test if connection is still alive
                try:
                    connection.voidcmd('NOOP')
                    pooled_conn.mark_used()
                    self._stats.record_connection_pool_hit()
                    self._logger.debug("Reused pooled FTPS connection")
                    
                except Exception:
                    # Connection is dead, create a new one
                    self._close_connection(connection)
                    connection = None
                    pooled_conn = None
                    
            except Empty:
                # Pool is empty
                pass
            
            # Create new connection if needed
            if connection is None:
                with self._lock:
                    if self._active_connections >= self.max_connections:
                        raise RuntimeError(f"Maximum connections ({self.max_connections}) reached")
                    self._active_connections += 1
                
                try:
                    connection = self._create_connection()
                    pooled_conn = PooledConnection(
                        connection=connection,
                        created_at=time.time(),
                        last_used=time.time()
                    )
                    self._stats.record_connection_pool_miss()
                    self._logger.debug("Created new FTPS connection")
                    
                except Exception:
                    with self._lock:
                        self._active_connections -= 1
                    raise
            
            yield connection
            
        except Exception:
            # Don't return broken connections to pool
            if connection:
                self._close_connection(connection)
                with self._lock:
                    self._active_connections -= 1
            raise
            
        else:
            # Return healthy connection to pool
            if pooled_conn and connection:
                if (not pooled_conn.is_expired(self.max_connection_age) and 
                    not pooled_conn.is_idle_too_long(self.max_idle_time)):
                    try:
                        self._pool.put_nowait(pooled_conn)
                    except Exception:
                        # Pool is full, close connection
                        self._close_connection(connection)
                        with self._lock:
                            self._active_connections -= 1
                else:
                    # Connection expired, close it
                    self._close_connection(connection)
                    with self._lock:
                        self._active_connections -= 1
    
    def _close_connection(self, connection: ftplib.FTP_TLS):
        """Safely close an FTPS connection."""
        try:
            connection.quit()
        except Exception:
            try:
                connection.close()
            except Exception:
                pass
    
    def _cleanup_expired(self):
        """Background thread to clean up expired connections."""
        while self._cleanup_running:
            try:
                time.sleep(30)  # Check every 30 seconds
                self._clean_pool()
            except Exception as e:
                self._logger.error(f"Error in connection cleanup: {e}")
    
    def _clean_pool(self):
        """Remove expired connections from the pool."""
        connections_to_close = []
        
        # Collect expired connections
        while True:
            try:
                pooled_conn = self._pool.get_nowait()
                if (pooled_conn.is_expired(self.max_connection_age) or 
                    pooled_conn.is_idle_too_long(self.max_idle_time)):
                    connections_to_close.append(pooled_conn.connection)
                else:
                    # Put back good connection
                    self._pool.put_nowait(pooled_conn)
            except Empty:
                break
        
        # Close expired connections
        for connection in connections_to_close:
            self._close_connection(connection)
            with self._lock:
                self._active_connections -= 1
        
        if connections_to_close:
            self._logger.debug(f"Cleaned up {len(connections_to_close)} expired FTPS connections")
    
    def close_all(self):
        """Close all connections and shutdown the pool."""
        self._cleanup_running = False
        
        # Close all pooled connections
        connections_to_close = []
        while True:
            try:
                pooled_conn = self._pool.get_nowait()
                connections_to_close.append(pooled_conn.connection)
            except Empty:
                break
        
        for connection in connections_to_close:
            self._close_connection(connection)
        
        with self._lock:
            self._active_connections = 0
        
        self._logger.debug(f"Closed {len(connections_to_close)} pooled FTPS connections")
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        with self._lock:
            return {
                "active_connections": self._active_connections,
                "max_connections": self.max_connections,
                "pool_size": self._pool.qsize(),
                "pool_utilization": round((self._active_connections / self.max_connections) * 100, 2)
            }


# Global connection pool
_connection_pool: Optional[FTPSConnectionPool] = None


def get_connection_pool(config: Dict[str, Any]) -> FTPSConnectionPool:
    """Get or create the global connection pool."""
    global _connection_pool
    if _connection_pool is None:
        file_concurrency = config.get("sftp", {}).get("ftps_file_concurrency", 1)
        segments_per_file = config.get("sftp", {}).get("ftps_segments", 4)
        # Need enough connections for: file_concurrency * segments_per_file + buffer
        max_connections = min(file_concurrency * segments_per_file + 2, 32)
        _connection_pool = FTPSConnectionPool(
            config=config["sftp"],
            max_connections=max_connections
        )
    return _connection_pool


def close_connection_pool():
    """Close the global connection pool."""
    global _connection_pool
    if _connection_pool is not None:
        _connection_pool.close_all()
        _connection_pool = None