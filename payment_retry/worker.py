"""One simulated worker owns the pending queue and at most one active attempt."""

from .scheduler import RetryJob, RetryScheduler


class RetryWorker:
    """Recognize acknowledgments by the sequence of the claimed queue entry."""

    def __init__(self) -> None:
        self._pending = RetryScheduler()
        self._active: RetryJob | None = None

    def schedule(self, job_id: str, due_at_ms: int) -> RetryJob:
        """Accept a new ID only when there is no pending or active copy."""
        # Claiming a job removes it from the queue's ID table. Check the
        # worker's active slot too before accepting the same ID again.
        if self._active is not None and self._active.job_id == job_id:
            raise ValueError(f"job already active: {job_id}")
        return self._pending.schedule(job_id, due_at_ms)

    def lookup_pending(self, job_id: str) -> RetryJob:
        return self._pending.get(job_id)

    def cancel_pending(self, job_id: str) -> RetryJob:
        return self._pending.cancel(job_id)

    def reschedule_pending(self, job_id: str, due_at_ms: int) -> RetryJob:
        """Keep the existing arrival order when a pending deadline changes."""
        return self._pending.reschedule(job_id, due_at_ms)

    def claim_due(self, now_ms: int) -> RetryJob | None:
        """Claim one eligible attempt. Resolve it before claiming another."""
        if self._active is not None:
            raise RuntimeError("worker already has an active attempt")
        # Move a due job into the worker's slot. When nothing is due,
        # pop_due returns None and the worker stays available.
        self._active = self._pending.pop_due(now_ms)
        return self._active

    def _match_active(self, token: int) -> RetryJob:
        # In the demo, both attempts are called A. Their tokens are 0 and 3.
        # A reply carrying 0 must leave attempt 3 available to finish.
        job = self._active
        if type(token) is not int or job is None or job.sequence != token:
            raise ValueError("unknown or stale attempt")
        return job

    def acknowledge(self, token: int) -> RetryJob:
        """Record simulated success and free the worker."""
        # Check the token before clearing the slot. A late reply raises
        # an error here while the current attempt stays active.
        job = self._match_active(token)
        self._active = None
        return job

    def retry_at(self, token: int, due_at_ms: int) -> RetryJob:
        """Return a failed attempt to the pending queue as a fresh arrival."""
        job = self._match_active(token)
        # Scheduling gives this retry a new token and a new place in line.
        # If its deadline is invalid, the old attempt is still in the slot.
        replacement = self._pending.schedule(job.job_id, due_at_ms)
        self._active = None
        return replacement
