# Payment Retry Scheduler

Ravi Shanker Thadishetti · MSCS-532 · Project Phase 2

This Python proof of concept schedules synthetic payment retries on a simulated clock. A chained hash table locates a pending job by ID. An indexed binary min-heap selects the earliest deadline. A single worker can claim a due job, acknowledge it, or return a failed attempt to the queue. All state lives in memory.

## Run

Use Python 3.10 or later. The project uses the standard library. Run these commands from the repository root.

```sh
python demo.py
python -m unittest discover -s tests -v
python compare_updates.py
```

In the demo, job A fails at 10 ms and returns at 20 ms. Job B already holds that deadline and runs first. The worker rejects a delayed acknowledgment for A’s first attempt and accepts the acknowledgment for its current attempt. The clock advances through explicit integer arguments, so the trace is repeatable.

## Implementation

- [Hash table](payment_retry/hash_table.py) resolves collisions with separate chaining and resizes at separate growth and shrink thresholds.
- [Scheduler](payment_retry/scheduler.py) maintains the heap, the ID index and a stable tie order. Rescheduling a pending job preserves arrival order. Retrying a claimed job creates a fresh arrival.
- [Worker](payment_retry/worker.py) owns the queue and one active attempt. An attempt token is unique for the lifetime of this worker. A successful acknowledgment frees the worker. A rejected acknowledgment leaves the active attempt intact.
- [Tests](tests/) exercise the queue, collisions, rescheduling, cancellation, stale acknowledgments and invalid requests. The model-based test checks 10,000 generated steps against a dictionary-and-minimum reference.
- [Report](report/Project_Phase2_Ravi_Shanker_Thadishetti.docx) explains the design, evidence and next steps.

## Results

All 15 tests passed. After 1,000 deadline changes to one of 32 pending jobs without polling, the indexed heap held 32 entries. The comparison heap, which appends a new revision and discards older ones during extraction, held 1,032. Both returned the same 32 jobs in the same order. This experiment counts heap entries. Timing, memory in bytes and restart recovery are outside its scope.

[Demo output](results/demo.txt) · [Update experiment](results/update_counts.txt) · [Test output](results/tests.txt)
