"""Automated tests for the replicated inventory simulation."""

from __future__ import annotations

import unittest

from client import InventoryClient
from inventory import PrimaryReplica, SecondaryReplica
from replication import ReplicationManager


class ReplicationTestCase(unittest.TestCase):
    """Shared setup/teardown for replication tests."""

    def setUp(self) -> None:
        self.primary = PrimaryReplica(initial_stock=10)
        self.secondary = SecondaryReplica(initial_stock=10)
        # Small delay keeps replication asynchronous without slowing tests.
        self.replication = ReplicationManager(secondary=self.secondary, delay=0.02)
        self.client = InventoryClient(self.primary, self.secondary, self.replication)

    def tearDown(self) -> None:
        # Ensure the worker always stops even if a test fails mid-way.
        if self.replication.is_paused():
            self.replication.resume()
        self.assertTrue(
            self.replication.shutdown(timeout=5.0),
            "Replication worker failed to shut down cleanly",
        )


class TestInitialState(ReplicationTestCase):
    def test_both_replicas_start_with_same_inventory(self) -> None:
        self.assertEqual(self.client.read_primary(), 10)
        self.assertEqual(self.client.read_secondary(), 10)


class TestPrimaryWrite(ReplicationTestCase):
    def test_write_updates_primary(self) -> None:
        self.client.update_stock(15)
        self.assertEqual(self.client.read_primary(), 15)


class TestSuccessfulReplication(ReplicationTestCase):
    def test_primary_update_eventually_propagates(self) -> None:
        self.client.update_stock(12)
        self.assertTrue(self.replication.wait_for_replication(timeout=5.0))
        self.assertEqual(self.client.read_primary(), 12)
        self.assertEqual(self.client.read_secondary(), 12)


class TestPauseAndStaleRead(ReplicationTestCase):
    def test_replication_can_be_paused(self) -> None:
        self.replication.pause()
        self.assertTrue(self.replication.is_paused())

    def test_secondary_returns_stale_value_while_paused(self) -> None:
        self.replication.pause()
        self.client.update_stock(9)
        # Give the worker a moment to attempt (and be blocked from) applying.
        # Synchronization-based check: pending must remain > 0 while paused.
        self.assertGreater(self.replication.pending_count(), 0)
        self.assertEqual(self.client.read_primary(), 9)
        self.assertEqual(self.client.read_secondary(), 10)

    def test_pending_updates_not_lost_while_paused(self) -> None:
        self.replication.pause()
        self.client.update_stock(7)
        self.assertGreater(self.replication.pending_count(), 0)
        self.assertEqual(self.client.read_secondary(), 10)

        self.replication.resume()
        self.assertTrue(self.replication.wait_for_replication(timeout=5.0))
        self.assertEqual(self.replication.pending_count(), 0)
        self.assertEqual(self.client.read_secondary(), 7)

    def test_primary_remains_writable_while_paused(self) -> None:
        self.replication.pause()
        self.client.update_stock(3)
        self.client.update_stock(4)
        self.assertEqual(self.client.read_primary(), 4)
        # Secondary still at initial value; writes did not block.
        self.assertEqual(self.client.read_secondary(), 10)


class TestRecoveryAndConvergence(ReplicationTestCase):
    def test_resume_causes_replicas_to_converge(self) -> None:
        self.replication.pause()
        self.client.update_stock(9)
        self.assertEqual(self.client.read_secondary(), 10)

        self.replication.resume()
        self.assertTrue(self.replication.wait_for_replication(timeout=5.0))
        self.assertEqual(self.client.read_primary(), 9)
        self.assertEqual(self.client.read_secondary(), 9)


class TestOrderedUpdates(ReplicationTestCase):
    def test_multiple_queued_updates_applied_in_order(self) -> None:
        self.replication.pause()
        for value in (1, 2, 3, 4, 5):
            self.client.update_stock(value)

        self.assertEqual(self.client.read_primary(), 5)
        self.assertEqual(self.client.read_secondary(), 10)
        self.assertEqual(self.replication.pending_count(), 5)

        self.replication.resume()
        self.assertTrue(self.replication.wait_for_replication(timeout=5.0))
        # Final secondary value must be the last enqueued update.
        self.assertEqual(self.client.read_secondary(), 5)
        self.assertEqual(self.client.read_primary(), 5)


class TestIndependentState(ReplicationTestCase):
    def test_replicas_maintain_independent_state(self) -> None:
        self.replication.pause()
        self.client.update_stock(42)
        # Mutating Primary must not mutate Secondary through shared state.
        self.assertEqual(self.primary.read(), 42)
        self.assertEqual(self.secondary.read(), 10)
        self.assertIsNot(self.primary, self.secondary)
        # Distinct backing values: secondary still holds its own copy.
        self.assertNotEqual(self.primary.read(), self.secondary.read())


class TestCleanShutdown(ReplicationTestCase):
    def test_replication_worker_shuts_down_cleanly(self) -> None:
        self.client.update_stock(11)
        self.assertTrue(self.replication.wait_for_replication(timeout=5.0))
        self.assertTrue(self.replication.shutdown(timeout=5.0))
        # Prevent tearDown from shutting down again.
        # Replace with a no-op-like manager already stopped: mark by calling
        # shutdown again should still be safe (worker already dead).
        self.assertTrue(self.replication.shutdown(timeout=1.0))


if __name__ == "__main__":
    unittest.main()
