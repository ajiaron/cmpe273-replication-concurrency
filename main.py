"""
Demonstration of asynchronous replication, stale reads, and eventual consistency.

Run:
    python main.py
"""

from __future__ import annotations

import sys

from client import InventoryClient
from inventory import PrimaryReplica, SecondaryReplica
from replication import ReplicationManager


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def run_scenario_1(client: InventoryClient, replication: ReplicationManager) -> None:
    """Successful asynchronous replication."""
    print("=== Scenario 1: Successful Replication ===\n")

    primary_stock = client.read_primary()
    secondary_stock = client.read_secondary()
    print("Initial state:")
    print(f"Primary stock: {primary_stock}")
    print(f"Secondary stock: {secondary_stock}")
    _assert(primary_stock == 10, "Primary should start at 10")
    _assert(secondary_stock == 10, "Secondary should start at 10")
    print()

    client.update_stock(12)
    _assert(client.read_primary() == 12, "Primary should immediately show 12")

    completed = replication.wait_for_replication(timeout=5.0)
    _assert(completed, "Replication did not complete within timeout")

    primary_read = client.read_primary()
    secondary_read = client.read_secondary()
    print()
    print(f"Primary read: {primary_read}")
    print(f"Secondary read: {secondary_read}")
    print()

    _assert(primary_read == 12, "Primary read expected 12")
    _assert(secondary_read == 12, "Secondary read expected 12")
    print("[PASS] Both replicas contain the same value.")
    print()

    # Restore baseline for Scenario 2.
    client.update_stock(10)
    completed = replication.wait_for_replication(timeout=5.0)
    _assert(completed, "Failed to restore stock=10 before Scenario 2")
    _assert(client.read_primary() == 10, "Primary should be restored to 10")
    _assert(client.read_secondary() == 10, "Secondary should be restored to 10")


def run_scenario_2(client: InventoryClient, replication: ReplicationManager) -> None:
    """Stale read while replication is paused."""
    print("=== Scenario 2: Stale Read ===\n")

    _assert(client.read_primary() == 10, "Primary should be 10 before Scenario 2")
    _assert(client.read_secondary() == 10, "Secondary should be 10 before Scenario 2")

    replication.pause()
    client.update_stock(9)

    # Do not wait for replication — it is paused, so Secondary must stay at 10.
    primary_read = client.read_primary()
    secondary_read = client.read_secondary()
    print()
    print(f"Primary read: {primary_read}")
    print(f"Secondary read: {secondary_read}")
    print()

    _assert(primary_read == 9, "Primary should return 9 while replication is paused")
    _assert(secondary_read == 10, "Secondary should still return 10 (stale)")
    print("[STALE READ] Secondary has not received the latest update.")
    print("[PASS] Stale read successfully demonstrated.")
    print()


def run_scenario_3(client: InventoryClient, replication: ReplicationManager) -> None:
    """Resume replication and demonstrate eventual consistency."""
    print("=== Scenario 3: Recovery and Convergence ===\n")

    replication.resume()
    completed = replication.wait_for_replication(timeout=5.0)
    _assert(completed, "Queued update did not propagate after resume")

    primary_read = client.read_primary()
    secondary_read = client.read_secondary()
    print()
    print(f"Primary read: {primary_read}")
    print(f"Secondary read: {secondary_read}")
    print()

    _assert(primary_read == 9, "Primary should still be 9")
    _assert(secondary_read == 9, "Secondary should converge to 9")
    print("[PASS] Both replicas have converged.")
    print("[PASS] Eventual consistency successfully demonstrated.")
    print()
    print("=== All 3 Scenarios Passed ===")


def main() -> int:
    primary = PrimaryReplica(initial_stock=10)
    secondary = SecondaryReplica(initial_stock=10)
    replication = ReplicationManager(secondary=secondary, delay=0.05, log=print)
    client = InventoryClient(primary, secondary, replication, log=print)

    try:
        run_scenario_1(client, replication)
        run_scenario_2(client, replication)
        run_scenario_3(client, replication)
    finally:
        shut_ok = replication.shutdown(timeout=5.0)
        if not shut_ok:
            print("Warning: replication worker did not shut down cleanly.", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
