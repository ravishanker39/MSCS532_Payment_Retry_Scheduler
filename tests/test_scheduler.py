import random
import unittest
from dataclasses import FrozenInstanceError

from payment_retry import RetryScheduler
from payment_retry.hash_table import ChainedHashTable


class HashTableTests(unittest.TestCase):
    def test_growth_shrink_overwrite_and_missing(self):
        table = ChainedHashTable()
        for number in range(1000):
            table[str(number)] = number
        self.assertGreater(table.capacity, 8)
        # Updating key 20 should replace its value even after a resize.
        # A second entry for that key would incorrectly raise the count.
        table["20"] = -20
        self.assertEqual(len(table), 1000)
        self.assertEqual(table["20"], -20)
        for number in range(1000):
            self.assertIn(str(number), table)
            del table[str(number)]
        self.assertEqual(table.capacity, 8)
        self.assertEqual(len(table), 0)
        with self.assertRaises(KeyError):
            del table["absent"]
        table.assert_valid()

    def test_all_keys_collide(self):
        # Returning zero puts every key in one bucket. After deleting the
        # even keys, each odd key must still resolve to its original value.
        table = ChainedHashTable(lambda _: 0)
        for number in range(200):
            table[str(number)] = number
        for number in range(0, 200, 2):
            del table[str(number)]
        for number in range(1, 200, 2):
            self.assertEqual(table[str(number)], number)
        self.assertEqual(len(table), 100)
        table.assert_valid()


class SchedulerTests(unittest.TestCase):
    def test_empty_and_deadline_boundary(self):
        queue = RetryScheduler()
        self.assertIsNone(queue.peek())
        self.assertIsNone(queue.pop_due(0))
        queue.schedule("a", 10)
        # Compare one tick before the deadline with the deadline itself.
        # The early poll must leave the job available for the second poll.
        self.assertIsNone(queue.pop_due(9))
        self.assertEqual(queue.pop_due(10).job_id, "a")
        self.assertIsNone(queue.pop_due(11))
        queue.assert_valid()

    def test_stable_ties_reschedule_and_reuse(self):
        queue = RetryScheduler()
        queue.schedule("a", 20)
        queue.schedule("b", 10)
        queue.reschedule("a", 10)
        # Moving pending a preserves its earlier arrival. Adding a again
        # after dispatch creates a new arrival behind b.
        self.assertEqual(queue.pop_due(10).job_id, "a")
        queue.schedule("a", 10)
        self.assertEqual(queue.pop_due(10).job_id, "b")
        self.assertEqual(queue.pop_due(10).job_id, "a")

    def test_deadline_changes_both_directions(self):
        queue = RetryScheduler()
        for number in range(50):
            queue.schedule(str(number), number)
        # Bring the last arrival forward and postpone the original head.
        # The full output order checks heap repair in both directions.
        queue.reschedule("49", 0)
        queue.reschedule("0", 100)
        queue.assert_valid()
        order = [queue.pop_due(100).job_id for _ in range(50)]
        self.assertEqual(order, ["49"] + [str(i) for i in range(1, 49)] + ["0"])

    def test_cancel_root_interior_and_last(self):
        # Ascending deadlines leave these entries in their insertion slots.
        # Rebuild the queue for each victim so those positions stay predictable.
        for victim in ("0", "4", "14"):
            queue = RetryScheduler()
            for number in range(15):
                queue.schedule(str(number), number)
            self.assertEqual(queue.cancel(victim).job_id, victim)
            queue.assert_valid()
            remaining = [queue.pop_due(100).job_id for _ in range(14)]
            self.assertEqual(remaining, [str(i) for i in range(15) if str(i) != victim])

    def test_invalid_and_duplicate_requests_preserve_state(self):
        queue = RetryScheduler()
        queue.schedule("a", 10)
        with self.assertRaises(ValueError):
            queue.schedule("a", 5)
        for bad in (-1, 2**63, 1.5, True, None):
            with self.assertRaises(ValueError):
                queue.reschedule("a", bad)
            with self.assertRaises(ValueError):
                queue.pop_due(bad)
            with self.assertRaises(ValueError):
                queue.schedule("new", bad)
        for bad in ("", "x" * 129, None, 12):
            with self.assertRaises(ValueError):
                queue.schedule(bad, 1)
        for operation in (queue.get, queue.cancel):
            with self.assertRaises(KeyError):
                operation("missing")
        with self.assertRaises(KeyError):
            queue.reschedule("missing", 1)
        # All rejected requests shared this queue. The original job should
        # still be the only one waiting, with its original deadline.
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue.peek().due_at_ms, 10)
        queue.assert_valid()

    def test_public_job_cannot_mutate_priority(self):
        queue = RetryScheduler()
        view = queue.schedule("a", 10)
        with self.assertRaises(FrozenInstanceError):
            view.due_at_ms = 0
        # A caller holding the old snapshot should still see 10, while a
        # fresh lookup sees the scheduler's updated deadline of 20.
        queue.reschedule("a", 20)
        self.assertEqual(view.due_at_ms, 10)
        self.assertEqual(queue.get("a").due_at_ms, 20)

    def test_model_based_operation_sequences(self):
        # Replay each seed twice, once with normal hashing and once with
        # every ID in one bucket. The same operations should work in both.
        for seed in range(10):
            for hasher in (hash, lambda _: 0):
                rng = random.Random(seed)
                queue = RetryScheduler(hasher=hasher)
                model = {}
                sequence = 0
                for _ in range(500):
                    job_id = f"j{rng.randrange(60)}"
                    due = rng.randrange(100)
                    action = rng.choice(("schedule", "reschedule", "cancel", "pop"))
                    if action == "schedule" and job_id not in model:
                        queue.schedule(job_id, due)
                        model[job_id] = (due, sequence)
                        sequence += 1
                    elif action == "reschedule" and job_id in model:
                        queue.reschedule(job_id, due)
                        model[job_id] = (due, model[job_id][1])
                    elif action == "cancel" and job_id in model:
                        self.assertEqual(queue.cancel(job_id).job_id, job_id)
                        del model[job_id]
                    elif action == "pop":
                        # The reference dictionary stores (deadline, arrival).
                        # Scanning it gives an independent answer to who is due.
                        first = min(model, key=model.get) if model else None
                        expected = first if first is not None and model[first][0] <= due else None
                        result = queue.pop_due(due)
                        self.assertEqual(result.job_id if result else None, expected)
                        if expected is not None:
                            del model[expected]
                    # Check both views after each step. The next job might be
                    # right even if another waiting job has the wrong deadline.
                    self.assertEqual(len(queue), len(model))
                    first = min(model, key=model.get) if model else None
                    self.assertEqual(queue.peek().job_id if queue.peek() else None, first)
                    for key, (deadline, arrival) in model.items():
                        self.assertEqual((queue.get(key).due_at_ms, queue.get(key).sequence),
                                         (deadline, arrival))
                    queue.assert_valid()


if __name__ == "__main__":
    unittest.main()
