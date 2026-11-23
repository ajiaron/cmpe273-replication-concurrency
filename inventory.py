"""Primary and Secondary inventory replica implementations."""

from __future__ import annotations

import threading


class PrimaryReplica:
    """Authoritative inventory replica that accepts client writes."""

    def __init__(self, initial_stock: int = 10) -> None:
        self._stock = initial_stock
        self._lock = threading.Lock()

    def write(self, value: int) -> int:
        """Update the authoritative stock value and return it."""
        with self._lock:
            self._stock = value
            return self._stock

    def read(self) -> int:
        """Return the current authoritative stock value."""
        with self._lock:
            return self._stock


class SecondaryReplica:
    """Read-only inventory replica updated only via replication."""

    def __init__(self, initial_stock: int = 10) -> None:
        self._stock = initial_stock
        self._lock = threading.Lock()

    def read(self) -> int:
        """Return the locally stored stock value (may be stale)."""
        with self._lock:
            return self._stock

    def apply_update(self, value: int) -> int:
        """Apply a replicated stock update. Called only by the replication worker."""
        with self._lock:
            self._stock = value
            return self._stock
