"""Compare physical heap entries during a burst of updates without polling."""

from heapq import heappop, heappush

from payment_retry import RetryScheduler


def run_comparison() -> tuple[list[tuple[int, int, int]], list[str]]:
    indexed = RetryScheduler()
    # A lazy heap appends each revision. The dictionary identifies the live one.
    lazy_heap: list[tuple[int, int, int, str]] = []
    latest: dict[str, int] = {}
    # Give both queues the same 32 jobs in the same arrival order. This
    # makes any later difference in entry counts traceable to the updates.
    for arrival in range(32):
        job_id = f"job-{arrival:02}"
        indexed.schedule(job_id, 100 + arrival)
        latest[job_id] = 0
        heappush(lazy_heap, (100 + arrival, arrival, 0, job_id))

    samples = [(0, len(indexed), len(lazy_heap))]
    for revision in range(1, 1001):
        # Move the same entry from one end of the ordering to the other.
        deadline = 10 if revision % 2 else 200
        indexed.reschedule("job-00", deadline)
        # The indexed queue moved its existing entry. The lazy queue keeps
        # its older entry and pushes a new one with the same arrival order.
        latest["job-00"] = revision
        heappush(lazy_heap, (deadline, 0, revision, "job-00"))
        if revision in (1, 100, 1000):
            samples.append((revision, len(indexed), len(lazy_heap)))

    order = []
    while lazy_heap:
        _, _, revision, job_id = heappop(lazy_heap)
        # Several popped entries may name job-00. Only its latest revision
        # belongs in the output, even if an older deadline comes up first.
        if latest.get(job_id) == revision:
            order.append(job_id)
            del latest[job_id]
    indexed_order = []
    # At 200 ms every live job is due. Compare the complete dispatch order
    # as well as its length so a missing or out-of-order job is caught.
    while (job := indexed.pop_due(200)) is not None:
        indexed_order.append(job.job_id)
    if indexed_order != order or len(order) != 32:
        raise AssertionError("queues disagreed on the final live jobs")
    return samples, order


if __name__ == "__main__":
    rows, order = run_comparison()
    print("updates,indexed_heap_entries,lazy_heap_entries")
    for row in rows:
        print(*row, sep=",")
    print(f"Drain matched for {len(order)} jobs. First {order[0]}, last {order[-1]}.")
