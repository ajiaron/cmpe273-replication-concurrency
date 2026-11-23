"""Client interface for reading and writing the replicated inventory system."""

from __future__ import annotations

from typing import Callable, Optional

from inventory import PrimaryReplica, SecondaryReplica
from replication import ReplicationManager


class InventoryClient:
    """
    Simple client that writes through the Primary and can read from either replica.

    Reads from Primary vs Secondary are explicit methods so callers must choose
    which consistency/latency tradeoff they want.
    """

    def __init__(
        self,
        primary: PrimaryReplica,
        secondary: SecondaryReplica,
        replication: ReplicationManager,
        log: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._primary = primary
        self._secondary = secondary
        self._replication = replication
        self._log = log or (lambda _msg: None)

    def update_stock(self, value: int) -> int:
        """Write a new stock value through the Primary and enqueue replication."""
        written = self._primary.write(value)
        self._log(f"[WRITE] Primary stock updated to {written}")
        self._replication.enqueue(written)
        return written

    def read_primary(self) -> int:
        """Read stock from the Primary (authoritative / latest value)."""
        return self._primary.read()

    def read_secondary(self) -> int:
        """Read stock from the Secondary (may be stale under async replication)."""
        return self._secondary.read()
