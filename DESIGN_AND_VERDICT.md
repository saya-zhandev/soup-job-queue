# Production Readiness Audit

**Audit date:** 2026-11-11

**Decision:** **DON'T SHIP at 100 `POST /jobs` requests/sec**

## Design note

PostgreSQL is the source of truth because job state, idempotency, leases, and
the execution counter must change safely under concurrent API and worker
activity. The schema gives each job a UUID, a unique idempotency key, a
constrained state, and a partial index for queued-job polling.

Workers claim jobs in a transaction using `SELECT ... FOR UPDATE SKIP LOCKED`,
then set `running`, their worker ID, and a 30-second lease using PostgreSQL
`NOW()`. A separate cleaner checks for expired running leases every five
seconds and requeues them in bounded batches. Completion and failure updates
must still match the running state, worker ID, and unexpired lease; this
rejects late writes from workers whose leases expired or were reassigned.

There is **no lease heartbeat/renewal**. A live job exceeding 30 seconds can
therefore be recovered and run again. Cancellation is an atomic transition
from `queued` to `cancelled`; a running or terminal job returns conflict.

The current execution side effect is a database increment of
`side_effect_count` in the same update that marks a job `succeeded`. The
intended execution contract is at-least-once, with idempotent side effects;
this counter does not prove exactly-once behavior for external systems.
External effects would need their own idempotency mechanism (for example, an
outbox or a downstream idempotency key).

## Chaos test proof

The final instrumented run used a fresh local PostgreSQL 15 database, eight
worker subprocesses, two simultaneous creates per idempotency key, and a
scheduled rate of 100 job-creation POSTs/sec. Worker kills occurred on a
two-second cycle. Cancellation and telemetry/list requests ran concurrently.
The POST rate is **not** the total HTTP request rate: cancellation and
monitoring requests are additional load.

Two runs occurred before persistent logging existed. Their displayed
terminal summaries are preserved, with that provenance clearly labeled, in
[`PRE_LOGGING_RUNS.json`](./PRE_LOGGING_RUNS.json); they are not treated as
byte-for-byte raw log files or used for the final verdict.

| Run prefix | Result | POST rate | POST latency p99 | Worker kills | Notes |
|---|---|---:|---:|---:|---|
| `chaos-7e3f8606c8974da398d5d2ee0b06b78f-` | Failed before load | n/a | n/a | 0 | Health probe tried to parse plain-text `/healthz` as JSON; 0 POSTs sent. |
| `chaos-106ab905e17a4c9b8547abfff8fb6907-` | 10,000 jobs, invariants passed | unmeasured | unmeasured | 32 | Pre-instrumentation; no fixed-rate scheduler or latency metrics. |
| `chaos-8f540120c8f946289de446d7e5b16dc9-` | 10,000 jobs, invariants passed | 99.92/sec | 230.948 ms | 32 | Earlier controlled run; sampled the API pool only. |
| `chaos-33a964e607b341efbf9b0d89f23bfa08-` | Aborted before load | 0 | n/a | 0 | Harness initially checked worker readiness only on stdout; Go logs readiness on stderr. No jobs were submitted. |
| `chaos-645acbbb4c26462d9ddef8c4237a3817-` | 10,000 jobs, invariants passed | 99.90/sec | 302.260 ms | 111 | Final run; separate stdout/stderr capture and API plus PostgreSQL connection telemetry. |

Final run summary:

| Metric | Observed |
|---|---:|
| Unique jobs / unique idempotency keys | 10,000 / 10,000 |
| `POST /jobs` requests | 20,000 (10,000 HTTP 201; 10,000 HTTP 200 retries) |
| POST latency p50 / p95 / p99 / max | 50.248 / 162.629 / 302.260 / 677.012 ms |
| POST request error rate | 0.0% |
| Cancellation requests | 11,421 (23 HTTP 200; 11,398 HTTP 409) |
| Final states | 9,977 succeeded; 23 cancelled |
| Side-effect total / jobs with >1 side effect | 9,977 / 0 |
| Running / queued / orphaned jobs at end | 0 / 0 / 0 |
| Worker kills / worker processes started | 111 / 119 |
| API connection pool | 25/25 peak in use; 19 of 413 samples at capacity |
| API pool wait delta / accumulated wait duration | 11,345 / 122,376.483 ms |
| PostgreSQL connections / configured maximum (peak) | 41 / 100 |
| Queue drain after final cancellation fan-out | 6.958 sec |
| Jobs requeued by periodic cleaner during chaos run | 0 |

The independently queried PostgreSQL state was `10,000` total rows,
`10,000` distinct idempotency keys, `0` running, `0` jobs with multiple
side effects, `0` orphaned rows, and `9,977` total side effects. The final
summary JSON, timestamped pool samples, HTTP status counts, worker kill
events, integration-test output, assertions, and captured worker stdout and
stderr are in [`harness_results.log`](./harness_results.log). The final run is
identified by the `chaos-645acbbb4c26462d9ddef8c4237a3817-` prefix.
Pre-logging run summaries are in `PRE_LOGGING_RUNS.json` with an explicit
notice that their original output was not persisted.

The measured 4.619 ms lease-recovery figure is the execution time of the
recovery SQL against a deliberately expired test row; it does **not** include
the 30-second lease or the cleaner's up-to-five-second polling delay. No
periodic lease-recovery event occurred during the final chaos load because
the emulated jobs normally complete too quickly to leave an in-flight job
behind when a worker is killed. Thus this run proves worker process kills and
the state invariants, but not end-to-end recovery latency for a long-running
job.

## Silent failure matrix

| Failure mode | Verification | Outcome and scope |
|---|---|---|
| Double claim | Hold a row lock in one transaction, then call the worker claim path. | **PASS.** The locked job was skipped in 17.3 ms; the second worker did not claim it or block on that row. |
| Zombie worker | Expire a running worker's lease, attempt completion, recover the row, and attempt completion again. | **PASS.** Both stale writes were rejected; recovery returned the job to `queued` and cleared worker/lease fields. The recovery SQL took 4.619 ms after expiry was already forced. |
| Cancel/complete race | Race the cancel HTTP handler with a worker claim and completion. | **PASS.** Final run of this test returned HTTP 409 for cancel and ended `succeeded` with one side effect. Either `cancelled` with zero effects or `succeeded` with one is valid. |
| Duplicate retries | Send 32 concurrent identical `POST /jobs` requests. | **PASS.** One HTTP 201 and 31 HTTP 200 responses referred to the same job. |
| Transaction isolation | In `REPEATABLE READ`, read a key range, insert a matching row in another transaction, and read again. | **PASS.** The repeatable-read transaction saw no phantom; the outside query saw the insert. This does not claim that `READ COMMITTED` prevents phantoms; the worker claim relies on row locking, not that property. |
| Clock skew | Claim a job and compare its lease against PostgreSQL `NOW()`. | **PASS, limited.** The lease was 29–30 seconds ahead of database time. The test verifies the SQL clock source but does not alter the host clock to emulate actual skew. |

## Objective production verdict

**DON'T SHIP at the stated 100 req/sec target.** The core correctness
invariants and all six focused integration checks passed, and the harness
sustained 99.90 job-creation POSTs/sec with no HTTP request errors. However,
the observed POST p99 was **302.260 ms**, above the stated 200 ms ship
threshold. The API's 25-connection pool reached capacity in 19 samples and
accumulated 11,345 waits totaling 122.4 seconds. The PostgreSQL server itself
peaked at 41 of 100 connections, so the observed exhaustion is at the
application pool, not the server's configured connection ceiling.

The p99 did not exceed 1 second, but meeting the more permissive threshold
does not satisfy the explicit `<200 ms` ship criterion. The queue drained in
6.958 seconds after the final cancellation fan-out; this is useful evidence,
not a defined production recovery SLA. These results are from one local
Docker run with a trivial database-only job effect and should not be treated
as a production capacity guarantee.

### Prioritized next steps

1. Profile API query time, `sql.DB` wait duration, and request concurrency.
   Reduce unnecessary database round trips and determine why the API pool
   saturates before increasing its limit.
2. Re-run the same 100 POST/sec test after each change and require p99 below
   200 ms with no pool waits; also test the intended cancellation mix.
3. Add a long-running job scenario that is killed after claim. Measure actual
   time from lease expiry through periodic recovery and completion.
4. Implement and test lease renewal for jobs that may exceed 30 seconds.
5. Test idempotency at any external side-effect boundary; the current counter
   only establishes database-local behavior.

## Reflective summary

Correctness held under 111 forced worker terminations, but the latency and
connection-pool telemetry changed the readiness decision: a clean invariant
result alone is not enough. Cancellation generated 11,421 requests, most
returning the expected conflict, so the reported p99 includes contention from
the combined test mix. Also, the worker task is deliberately short; the lack
of observed periodic recovery events is a coverage gap, not evidence that
long-running crash recovery is fast. Treat the verdict as a local, repeatable
engineering signal and validate again against realistic payload execution,
job duration, hardware, and production traffic composition.
