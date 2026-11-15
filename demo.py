"""Replay a payment retry using synthetic job IDs and an integer clock."""

from payment_retry.worker import RetryWorker


def main() -> None:
    worker = RetryWorker()
    worker.schedule("A", 30)
    worker.schedule("B", 20)
    worker.schedule("C", 15)
    print("Pending B:", worker.lookup_pending("B").due_at_ms, "ms")
    print("Move A earlier:", worker.reschedule_pending("A", 10).due_at_ms, "ms")
    print("Cancel before dispatch:", worker.cancel_pending("C").job_id)
    print("Claim at 9 ms:", worker.claim_due(9))

    first = worker.claim_due(10)
    assert first is not None
    print(f"Claim at 10 ms: {first.job_id}, token {first.sequence}")
    retry = worker.retry_at(first.sequence, 20)
    print(f"Requeue A at {retry.due_at_ms} ms, token {retry.sequence}")

    # B arrived before A's replacement. They now share a deadline.
    second = worker.claim_due(20)
    assert second is not None
    print("First at 20 ms:", worker.acknowledge(second.sequence).job_id)
    last = worker.claim_due(20)
    assert last is not None
    print(f"Next at 20 ms: {last.job_id}, token {last.sequence}")
    try:
        worker.acknowledge(first.sequence)
    except ValueError as exc:
        print("Late acknowledgment:", exc)
    print("Acknowledge current attempt:", worker.acknowledge(last.sequence).job_id)
    print("Final claim:", worker.claim_due(20))


if __name__ == "__main__":
    main()
