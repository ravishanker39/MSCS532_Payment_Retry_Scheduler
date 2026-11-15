"""One simulated worker owns the pending queue and at most one active attempt."""

from .scheduler import RetryJob, RetryScheduler


class RetryWorker:
    """Recognize acknowledgments by the sequence of the claimed queue entry."""

    def __init__(self) -> None:
        self._pending = RetryScheduler()
        self._active: RetryJob | None = None

    def schedule(self, job_id: str, due_at_ms: int) -> RetryJob:
        """Accept a new ID only when there is no pending or active copy."""
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
        self._active = self._pending.pop_due(now_ms)
        return self._active

    def _match_active(self, token: int) -> RetryJob:
        # Reusing a job ID is legitimate. Only the matching attempt may finish.
        job = self._active
        if type(token) is not int or job is None or job.sequence != token:
            raise ValueError("unknown or stale attempt")
        return job

    def acknowledge(self, token: int) -> RetryJob:
        """Record simulated success and free the worker."""
        job = self._match_active(token)
        self._active = None
        return job

    def retry_at(self, token: int, due_at_ms: int) -> RetryJob:
        """Return a failed attempt to the pending queue as a fresh arrival."""
        job = self._match_active(token)
        # Enqueue first. An invalid deadline leaves the current attempt active.
        replacement = self._pending.schedule(job.job_id, due_at_ms)
        self._active = None
        return replacement
