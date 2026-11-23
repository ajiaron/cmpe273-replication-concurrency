"""Asynchronous replication from Primary to Secondary."""

from __future__ import annotations

import queue
import threading
import time
from typing import Callable, Optional

from inventory import SecondaryReplica


class ReplicationManager:
    """
    Manages asynchronous, ordered replication of inventory updates.

    Uses a thread-safe FIFO queue and a background worker. Pausing prevents
    updates from being applied while retaining them in the queue until resume.
    """

    def __init__(
        self,
        secondary: SecondaryReplica,
        delay: float = 0.05,
        log: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._secondary = secondary
        self._delay = delay
        self._log = log or (lambda _msg: None)

        # FIFO queue of stock values to apply on the secondary.
        self._queue: queue.Queue[Optional[int]] = queue.Queue()

        # When cleared, the worker blocks and does not apply updates.
        self._can_replicate = threading.Event()
        self._can_replicate.set()

        self._shutdown = threading.Event()

        # Tracks enqueued-but-not-yet-applied updates for wait_for_replication.
        self._pending = 0
        self._condition = threading.Condition()

        self._worker = threading.Thread(
            target=self._worker_loop,
            name="replication-worker",
            daemon=True,
        )
        self._worker.start()

    def enqueue(self, value: int) -> None:
        """Enqueue a stock update for asynchronous replication (preserves order)."""
        with self._condition:
            self._pending += 1
        self._queue.put(value)
        self._log("[REPLICATION] Update queued")

    def pause(self) -> None:
        """Pause replication. Pending updates remain queued until resume."""
        self._can_replicate.clear()
        self._log("[REPLICATION] Paused")

    def resume(self) -> None:
        """Resume replication so queued updates can be applied in order."""
        self._can_replicate.set()
        self._log("[REPLICATION] Resumed")

    def is_paused(self) -> bool:
        """Return True if replication is currently paused."""
        return not self._can_replicate.is_set()

    def wait_for_replication(self, timeout: float = 5.0) -> bool:
        """
        Block until all currently pending updates have been applied.

        Returns True if replication completed within the timeout, else False.
        If replication is paused, this waits until resume + drain (or timeout).
        """
        deadline = time.monotonic() + timeout
        with self._condition:
            while self._pending > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)
            return True

    def pending_count(self) -> int:
        """Return the number of updates not yet applied to the Secondary."""
        with self._condition:
            return self._pending

    def shutdown(self, timeout: float = 5.0) -> bool:
        """
        Signal the worker to stop and wait for a clean exit.

        Returns True if the worker thread terminated within the timeout.
        """
        self._shutdown.set()
        # Unblock a paused worker so it can observe the shutdown signal.
        self._can_replicate.set()
        # Sentinel wakes the worker if it is blocked on an empty queue.
        self._queue.put(None)
        self._worker.join(timeout=timeout)
        return not self._worker.is_alive()

    def _worker_loop(self) -> None:
        """Background loop: dequeue updates and apply them when not paused."""
        while not self._shutdown.is_set():
            try:
                # Short timeout so we can periodically check shutdown.
                item = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue

            # Sentinel used during shutdown.
            if item is None:
                self._queue.task_done()
                if self._shutdown.is_set():
                    break
                continue

            # Pause gate: do not apply until replication is resumed.
            # Wait in a loop so we can still respond to shutdown.
            while not self._can_replicate.is_set():
                if self._shutdown.is_set():
                    # Re-queue the item so pending accounting stays honest if needed,
                    # but during shutdown we mark it done and exit.
                    with self._condition:
                        self._pending = max(0, self._pending - 1)
                        self._condition.notify_all()
                    self._queue.task_done()
                    return
                self._can_replicate.wait(timeout=0.1)

            if self._shutdown.is_set():
                with self._condition:
                    self._pending = max(0, self._pending - 1)
                    self._condition.notify_all()
                self._queue.task_done()
                break

            # Simulated network / replication lag.
            if self._delay > 0:
                time.sleep(self._delay)

            applied = self._secondary.apply_update(item)
            self._log(f"[REPLICATION] Secondary applied stock = {applied}")

            with self._condition:
                self._pending -= 1
                if self._pending < 0:
                    self._pending = 0
                self._condition.notify_all()

            self._queue.task_done()
