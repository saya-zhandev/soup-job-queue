# Soup Job Queue

A Go `net/http` job API backed by PostgreSQL 15, with idempotent job creation,
four background workers, leased claims, and a local chaos-test harness.

## Requirements

- Docker Desktop (Windows/macOS) or Docker Engine (Linux) with Docker Compose
  v2.
- For the chaos test only: Go 1.23 or later and Python 3.10 or later on the
  host. The first build needs internet access to download Go modules unless
  they are already cached.
- Run all commands from the repository root.

## Run the application

Clone the repository, then start the services:

```sh
git clone https://github.com/saya-zhandev/soup-job-queue.git
cd soup-job-queue
docker compose up --build -d
docker compose ps
```

Compose starts PostgreSQL and waits for its health check before starting the
API. On a **new/empty** PostgreSQL volume, Compose applies `schema.sql`
automatically. The default development ports are API `8080` and PostgreSQL
`5432`; the development database is `job_queue`, user `soup`, password
`soup_password`. Do not use these credentials outside local development.

Check the API and app logs:

```sh
curl http://localhost:8080/healthz
docker compose logs -f app
```

`/healthz` returns `ok` when the HTTP server is up. To stop the app while
keeping its database volume, run `docker compose down`. The command
`docker compose down -v` deletes the database volume and all stored jobs.

Schema initialization scripts run only when PostgreSQL initializes an empty
volume. If you have an older volume that was created before the schema was
mounted, apply it once with:

```powershell
docker compose exec -T postgres psql -U soup -d job_queue `
  -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/001-schema.sql
```

Do not run `schema.sql` again if its `jobs` table and `job_status` type already
exist; it is an initial schema, not a repeatable migration.

## Jobs API

- `POST /jobs` creates or returns a job. Required header:
  `Idempotency-Key: <unique-key>`. JSON body:
  `{"payload":{"task":"train"}}`. A new job returns `201`; a retry using the
  same key returns the existing job with `200`.
- `GET /jobs/{id}` retrieves a job by UUID.
- `GET /jobs?limit=20&offset=0` lists jobs. `limit` defaults to 20 and is
  capped at 100.
- `POST /jobs/{id}/cancel` cancels a queued job. Jobs already running or
  terminal return `409 Conflict`.

PowerShell smoke test:

```powershell
$headers = @{ "Idempotency-Key" = "local-smoke-001" }
$body = '{"payload":{"task":"train"}}'
$job = Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/jobs" `
  -Headers $headers -ContentType "application/json" -Body $body
$job
Invoke-RestMethod -Uri "http://localhost:8080/jobs/$($job.id)"
```

The application starts four workers. They claim jobs with
`FOR UPDATE SKIP LOCKED`, use a 30-second lease, and requeue expired leases
with a cleaner that polls every five seconds. The lease is not renewed while
a job runs. The current simulated side effect increments
`side_effect_count` in PostgreSQL.

## Run the local chaos audit

The harness builds the Go worker binary, runs six PostgreSQL integration
checks, starts eight worker subprocesses, submits two simultaneous creates
for each of 10,000 idempotency keys at a scheduled 100 `POST /jobs` requests
per second, sends cancellation requests, and force-terminates/restarts one of
its workers every two seconds. On Windows, forced termination uses
`TerminateProcess`; on Unix-like systems it uses the platform's process-kill
behavior. The harness only accepts loopback API/database addresses and only
terminates worker processes it launched.

Use a **fresh, disposable Compose project/database**. The project name gives
it a separate network and volume; the alternate ports avoid colliding with
the normal app. The schema is automatically applied to the new volume:

```powershell
$env:WORKER_COUNT = "0"
$env:ENABLE_DB_STATS_ENDPOINT = "true"
$env:POSTGRES_DB = "job_queue_audit"
$env:APP_HOST_PORT = "18080"
$env:POSTGRES_HOST_PORT = "15433"
docker compose -p soup-job-queue-audit up --build -d
python chaos_test.py `
  --api-url "http://127.0.0.1:18080" `
  --database-url "postgres://soup:soup_password@127.0.0.1:15433/job_queue_audit?sslmode=disable"
Remove-Item Env:WORKER_COUNT, Env:ENABLE_DB_STATS_ENDPOINT, Env:POSTGRES_DB, `
  Env:APP_HOST_PORT, Env:POSTGRES_HOST_PORT
```

If either host port is already in use, choose unused values and use the same
ports in both `--api-url` and `--database-url`. The pool-stats endpoint is
opt-in and is enabled here only to measure the local audit. Do not enable it
for a public deployment.

The defaults are 10,000 jobs, eight workers, and 100 job-creation POSTs/sec.
The POST rate is not the total HTTP rate: cancellation, listing, and telemetry
requests add more traffic. A run takes several minutes. The script prints a
JSON summary and appends the summary, HTTP status counts, POST latency
percentiles, pool samples, worker kill events, integration-test output,
assertions, and worker stdout/stderr to `harness_results.log` in the
repository. The log is append-only across runs; find a run by its `run_prefix`
and its `final_summary` entry. Pre-instrumentation run output is labeled
separately in [`PRE_LOGGING_RUNS.json`](./PRE_LOGGING_RUNS.json). Job rows are
retained for inspection.

The current measured audit and its limitations are documented in
[`DESIGN_AND_VERDICT.md`](./DESIGN_AND_VERDICT.md). Its verdict is **DON'T
SHIP** at the tested rate: invariants passed, but measured POST p99 exceeded
the 200 ms target and the API connection pool recorded waits.
