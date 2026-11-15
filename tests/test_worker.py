import unittest

from compare_updates import run_comparison
from payment_retry.worker import RetryWorker


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.worker = RetryWorker()

    def test_gate_at_deadline_and_busy_worker(self):
        self.worker.schedule("A", 10)
        self.assertIsNone(self.worker.claim_due(9))
        job = self.worker.claim_due(10)
        self.assertEqual(job.job_id, "A")
        # A has left the queue but still belongs to the worker. Until it
        # finishes, a second claim and a second copy of A must both fail.
        with self.assertRaises(RuntimeError):
            self.worker.claim_due(11)
        with self.assertRaises(ValueError):
            self.worker.schedule("A", 11)
        self.assertEqual(self.worker.acknowledge(job.sequence), job)
        self.assertIsNone(self.worker.claim_due(11))

    def test_failure_rejoins_tie_as_new_arrival(self):
        self.worker.schedule("A", 1)
        self.worker.schedule("B", 2)
        first = self.worker.claim_due(1)
        retry = self.worker.retry_at(first.sequence, 2)
        # B was already waiting at time 2. Requeuing A at that same time
        # must put its new attempt behind B.
        self.assertGreater(retry.sequence, first.sequence)
        older = self.worker.claim_due(2)
        self.assertEqual(older.job_id, "B")
        self.worker.acknowledge(older.sequence)
        self.assertEqual(self.worker.claim_due(2), retry)

    def test_stale_ack_and_retry_leave_current_attempt_active(self):
        self.worker.schedule("A", 0)
        first = self.worker.claim_due(0)
        current = self.worker.retry_at(first.sequence, 4)
        self.assertEqual(self.worker.claim_due(4), current)
        # Send both sorts of late reply for A's first attempt. Accepting the
        # current token afterward proves those errors left the active slot intact.
        with self.assertRaises(ValueError):
            self.worker.acknowledge(first.sequence)
        with self.assertRaises(ValueError):
            self.worker.retry_at(first.sequence, 5)
        self.assertEqual(self.worker.acknowledge(current.sequence), current)
        with self.assertRaises(ValueError):
            self.worker.acknowledge(current.sequence)

    def test_invalid_retry_preserves_active_attempt(self):
        self.worker.schedule("A", 0)
        job = self.worker.claim_due(0)
        with self.assertRaises(ValueError):
            self.worker.retry_at(job.sequence, -1)
        # The first token is 0. Python treats False as equal to 0, so
        # checking this case catches an accidental acknowledgment.
        with self.assertRaises(ValueError):
            self.worker.acknowledge(False)
        self.assertEqual(self.worker.acknowledge(job.sequence), job)

    def test_lookup_cancel_and_reschedule_before_claim(self):
        self.worker.schedule("A", 3)
        self.worker.schedule("B", 8)
        self.worker.reschedule_pending("B", 1)
        self.assertEqual(self.worker.lookup_pending("B").due_at_ms, 1)
        # B is now due first. Cancelling it should remove it from both the
        # ID lookup and the dispatch order, leaving just A at time 3.
        self.assertEqual(self.worker.cancel_pending("B").job_id, "B")
        with self.assertRaises(KeyError):
            self.worker.lookup_pending("B")
        self.assertIsNone(self.worker.claim_due(1))
        self.assertEqual(self.worker.claim_due(3).job_id, "A")

    def test_indexed_and_lazy_heaps_agree_after_update_burst(self):
        samples, order = run_comparison()
        # Only 32 jobs are live. The lazy heap still has one old entry for
        # each edit. The final edit moved job-00 behind every other job.
        self.assertEqual(samples[-1], (1000, 32, 1032))
        self.assertEqual(order, [f"job-{n:02}" for n in range(1, 32)] + ["job-00"])
