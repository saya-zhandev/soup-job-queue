#!/usr/bin/env python3
"""Local-only kill/restart test for the job queue."""

import argparse
import concurrent.futures
import ipaddress
import json
import math
import os
import random
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import ProxyHandler, Request, build_opener


TERMINAL_STATES = {"succeeded", "failed", "cancelled"}
WORKER_READY_MARKER = "started 1 job workers and the lease recovery cleaner"
LOCAL_OPENER = build_opener(ProxyHandler({}))


def require_loopback(host, label):
    if host.lower() == "localhost":
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ValueError(f"{label} must use localhost or a loopback IP address") from exc
    if not address.is_loopback:
        raise ValueError(f"{label} must use localhost or a loopback IP address")


def percentile(sorted_values, quantile):
    if not sorted_values:
        return None
    index = max(0, math.ceil(quantile * len(sorted_values)) - 1)
    return round(sorted_values[index], 3)


def summarize_pool_samples(samples):
    if not samples:
        return {"available": False, "sample_count": 0}
    max_open = max(int(sample.get("max_open_connections", 0)) for sample in samples)
    max_in_use = max(int(sample.get("in_use_connections", 0)) for sample in samples)
    max_utilization = max(
        (
            int(sample.get("in_use_connections", 0)) / max_open
            for sample in samples
            if max_open > 0
        ),
        default=0,
    )
    first_wait = int(samples[0].get("wait_count", 0))
    last_wait = int(samples[-1].get("wait_count", first_wait))
    first_wait_ms = float(samples[0].get("wait_duration_ms", 0))
    last_wait_ms = float(samples[-1].get("wait_duration_ms", first_wait_ms))
    database_max_connections = max(
        int(sample.get("database_max_connections", 0)) for sample in samples
    )
    max_database_connections = max(
        int(sample.get("database_connections", 0)) for sample in samples
    )
    return {
        "available": True,
        "sample_count": len(samples),
        "max_open_connections": max_open,
        "max_in_use_connections": max_in_use,
        "max_utilization": round(max_utilization, 4),
        "saturated_samples": sum(
            1
            for sample in samples
            if int(sample.get("max_open_connections", 0)) > 0
            and int(sample.get("in_use_connections", 0))
            >= int(sample.get("max_open_connections", 0))
        ),
        "wait_count_delta": max(0, last_wait - first_wait),
        "wait_duration_delta_ms": round(max(0, last_wait_ms - first_wait_ms), 3),
        "max_database_connections": max_database_connections,
        "database_max_connections": database_max_connections,
        "max_database_utilization": round(
            max_database_connections / database_max_connections
            if database_max_connections > 0 else 0,
            4,
        ),
    }


def request_json(url, method="GET", headers=None, body=None, timeout=30):
    request = Request(url, data=body, headers=headers or {}, method=method)
    try:
        response = LOCAL_OPENER.open(request, timeout=timeout)
    except HTTPError as exc:
        response = exc
    except (TimeoutError, URLError) as exc:
        raise RuntimeError(f"{method} {url}: {exc}") from exc

    with response:
        raw_body = response.read()
        if not raw_body:
            return response.status, None
        try:
            return response.status, json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{method} {url}: invalid JSON response") from exc


class ChaosRun:
    def __init__(self, args, root, binary, temp_dir, run_prefix):
        self.args = args
        self.root = root
        self.binary = binary
        self.temp_dir = Path(temp_dir)
        self.api_url = args.api_url.rstrip("/")
        self.run_prefix = run_prefix
        self.log_path = root / "harness_results.log"
        self.stop_chaos = threading.Event()
        self.stop_cancellations = threading.Event()
        self.stop_metrics = threading.Event()
        self.worker_lock = threading.Lock()
        self.ids_lock = threading.Lock()
        self.errors_lock = threading.Lock()
        self.counts_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.metrics_lock = threading.Lock()
        self.worker_processes = [None] * args.workers
        self.worker_log_paths = [None] * args.workers
        self.worker_stderr_paths = [None] * args.workers
        self.worker_log_records = []
        self.worker_restarts = [0] * args.workers
        self.created_ids = {}
        self.errors = []
        self.error_count = 0
        self.request_error_count = 0
        self.request_counts = Counter()
        self.http_status_counts = Counter()
        self.post_latencies_ms = []
        self.pool_samples = []
        self.pool_wait_start = None
        self.worker_kills = 0
        self.unexpected_worker_exits = 0
        self.integration_checks = 0
        self.worker_recovery_events = 0
        self.lease_recovery_query_ms = None
        self.chaos_thread = None
        self.cancellation_thread = None
        self.metrics_thread = None
        self.load_started_at = None
        self.load_finished_at = None
        self.queue_drain_seconds = None
        with self.log_path.open("a", encoding="utf-8") as log_file:
            log_file.write(
                f"\n=== audit run {datetime.now(timezone.utc).isoformat()} "
                f"prefix={run_prefix} target_post_rps={args.post_rps} ===\n"
            )

        self.worker_env = os.environ.copy()
        if args.database_url:
            self.worker_env["DATABASE_URL"] = args.database_url

    def log_line(self, channel, message):
        line = f"{datetime.now(timezone.utc).isoformat()} [{channel}] {message}"
        with self.log_lock:
            with self.log_path.open("a", encoding="utf-8") as log_file:
                log_file.write(line + "\n")

    def record_error(self, message, request_error=False):
        with self.errors_lock:
            self.error_count += 1
            if request_error:
                self.request_error_count += 1
            if len(self.errors) < 20:
                self.errors.append(message)
        self.log_line("stderr", message)

    def record_http(self, endpoint, status, elapsed_ms=None):
        with self.counts_lock:
            self.request_counts[endpoint] += 1
            status_label = str(status) if status is not None else "error"
            self.http_status_counts[f"{endpoint} {status_label}"] += 1
            if endpoint == "POST /jobs" and elapsed_ms is not None:
                self.post_latencies_ms.append(elapsed_ms)
            if status is None or status >= 500:
                self.request_error_count += 1

    def call_api(self, *args, **kwargs):
        kwargs.setdefault("timeout", self.args.request_timeout)
        method = kwargs.get("method", "GET").upper()
        path = urlparse(args[0]).path
        if method == "POST" and path == "/jobs":
            endpoint = "POST /jobs"
        elif method == "POST" and path.endswith("/cancel"):
            endpoint = "POST /jobs/{id}/cancel"
        elif method == "GET" and path.startswith("/jobs/"):
            endpoint = "GET /jobs/{id}"
        else:
            endpoint = f"{method} {path}"
        started = time.perf_counter()
        try:
            status, value = request_json(*args, **kwargs)
        except RuntimeError:
            self.record_http(endpoint, None, (time.perf_counter() - started) * 1000)
            raise
        self.record_http(endpoint, status, (time.perf_counter() - started) * 1000)
        return status, value

    def check_api(self):
        deadline = time.monotonic() + self.args.startup_timeout
        while time.monotonic() < deadline:
            try:
                request = Request(f"{self.api_url}/healthz")
                with LOCAL_OPENER.open(request, timeout=self.args.request_timeout) as response:
                    if response.status == 200:
                        response.read()
                        self.log_line("stdout", f"API ready: {self.api_url}/healthz")
                        return
            except (TimeoutError, URLError):
                time.sleep(0.5)
        raise RuntimeError(f"API health check failed: {self.api_url}/healthz")

    def start_worker(self, index):
        restart = self.worker_restarts[index]
        log_path = self.temp_dir / f"worker-{index + 1}-run-{restart + 1}.log"
        stderr_path = self.temp_dir / f"worker-{index + 1}-run-{restart + 1}.stderr.log"
        log_file = log_path.open("ab")
        stderr_file = stderr_path.open("ab")
        try:
            process = subprocess.Popen(
                [str(self.binary), "worker"],
                cwd=self.root,
                env=self.worker_env,
                stdout=log_file,
                stderr=stderr_file,
            )
        finally:
            log_file.close()
            stderr_file.close()

        with self.worker_lock:
            self.worker_processes[index] = process
            self.worker_log_paths[index] = log_path
            self.worker_stderr_paths[index] = stderr_path
            self.worker_restarts[index] += 1
            self.worker_log_records.append((index, log_path, stderr_path))
        self.log_line(
            "stdout",
            f"worker start index={index + 1} pid={process.pid} restart={restart + 1}",
        )
        return process, log_path

    def wait_for_workers(self):
        deadline = time.monotonic() + self.args.startup_timeout
        pending = set(range(self.args.workers))
        while pending and time.monotonic() < deadline:
            for index in tuple(pending):
                with self.worker_lock:
                    process = self.worker_processes[index]
                    log_path = self.worker_log_paths[index]
                    stderr_path = self.worker_stderr_paths[index]
                if process.poll() is not None:
                    output = log_path.read_text(errors="replace") if log_path.exists() else ""
                    if stderr_path.exists():
                        output += stderr_path.read_text(errors="replace")
                    raise RuntimeError(f"worker {index + 1} exited during startup:\n{output[-4000:]}")
                output = log_path.read_text(errors="replace") if log_path.exists() else ""
                if stderr_path.exists():
                    output += stderr_path.read_text(errors="replace")
                if WORKER_READY_MARKER in output:
                    pending.remove(index)
            if pending:
                time.sleep(0.1)
        if pending:
            raise RuntimeError(f"workers did not become ready: {sorted(pending)}")

    def start_chaos(self):
        self.chaos_thread = threading.Thread(target=self.chaos_loop, name="worker-chaos")
        self.chaos_thread.start()
        self.cancellation_thread = threading.Thread(
            target=self.cancellation_loop, name="concurrent-cancellations"
        )
        self.cancellation_thread.start()
        self.metrics_thread = threading.Thread(target=self.db_metrics_loop, name="db-pool-metrics")
        self.metrics_thread.start()

    def db_metrics_loop(self):
        while not self.stop_metrics.is_set():
            try:
                status, stats = self.call_api(f"{self.api_url}/debug/db-stats")
                if status != 200 or not isinstance(stats, dict):
                    self.record_error(
                        f"database pool telemetry returned HTTP {status}",
                        request_error=True,
                    )
                    return
                with self.metrics_lock:
                    self.pool_samples.append(stats)
                self.log_line("stdout", f"db_pool_sample {json.dumps(stats, sort_keys=True)}")
            except RuntimeError as exc:
                self.record_error(f"database pool telemetry failed: {exc}", request_error=True)
                return
            self.stop_metrics.wait(0.5)

    def chaos_loop(self):
        while not self.stop_chaos.wait(2):
            index = None
            process = None
            process = None
            try:
                for index in range(len(self.worker_processes)):
                    with self.worker_lock:
                        process = self.worker_processes[index]
                    if process is not None and process.poll() is not None:
                        with self.counts_lock:
                            self.unexpected_worker_exits += 1
                        self.record_error(f"worker {index + 1} exited unexpectedly")
                        self.start_worker(index)

                with self.worker_lock:
                    active = [
                        index
                        for index, process in enumerate(self.worker_processes)
                        if process is not None and process.poll() is None
                    ]
                    if not active:
                        self.record_error("all worker subprocesses exited")
                        continue
                    index = random.choice(active)
                    process = self.worker_processes[index]

                process.kill()
                process.wait(timeout=10)
                with self.counts_lock:
                    self.worker_kills += 1
                self.log_line(
                    "stdout",
                    f"worker kill index={index + 1} pid={process.pid} "
                    f"returncode={process.returncode}",
                )
                self.start_worker(index)
            except Exception as exc:
                self.record_error(f"chaos controller failed: {exc}", request_error=False)
                if index is not None and process is not None and process.poll() is not None:
                    try:
                        self.start_worker(index)
                    except Exception as restart_error:
                        self.record_error(f"could not restart worker {index + 1}: {restart_error}")

    def cancellation_loop(self):
        while not self.stop_cancellations.is_set():
            with self.ids_lock:
                job_ids = list(self.created_ids.values())
            if not job_ids:
                self.stop_cancellations.wait(0.05)
                continue
            try:
                self.cancel_job(random.choice(job_ids))
            except Exception as exc:
                self.record_error(f"cancellation loop failed: {exc}")
            self.stop_cancellations.wait(0.1)

    def create_job(self, sequence):
        key = f"{self.run_prefix}{sequence}"
        payload = json.dumps({"payload": {"task": "chaos", "sequence": sequence}}).encode()
        status, response = self.call_api(
            f"{self.api_url}/jobs",
            method="POST",
            headers={"Content-Type": "application/json", "Idempotency-Key": key},
            body=payload,
        )
        if status not in (200, 201) or not isinstance(response, dict) or not response.get("id"):
            message = f"create for key {key} returned HTTP {status}: {response}"
            self.record_error(message, request_error=True)
            raise RuntimeError(message)
        if response.get("idempotency_key") != key:
            message = f"create response returned the wrong idempotency key for {key}"
            self.record_error(message, request_error=True)
            raise RuntimeError(message)
        with self.ids_lock:
            self.created_ids[key] = response["id"]
        return key

    def cancel_job(self, job_id):
        try:
            status, _ = self.call_api(
                f"{self.api_url}/jobs/{job_id}/cancel",
                method="POST",
            )
            if status not in (200, 409):
                self.record_error(f"cancel for {job_id} returned HTTP {status}", request_error=True)
        except RuntimeError as exc:
            self.record_error(str(exc))

    def submit_duplicate_posts(self):
        futures = []
        request_interval = 2 / self.args.post_rps
        due_at = time.perf_counter()
        self.load_started_at = due_at
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.concurrency) as executor:
            for sequence in range(self.args.jobs):
                futures.append(executor.submit(self.create_job, sequence))
                futures.append(executor.submit(self.create_job, sequence))
                due_at += request_interval
                delay = due_at - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    self.record_error(f"POST request failed: {exc}")
        self.load_finished_at = time.perf_counter()
        elapsed = self.load_finished_at - self.load_started_at
        observed_rps = self.request_counts["POST"] / elapsed if elapsed > 0 else 0
        self.log_line(
            "stdout",
            f"POST load complete target_rps={self.args.post_rps:g} "
            f"observed_rps={observed_rps:.2f} requests={self.request_counts['POST /jobs']} "
            f"duration_seconds={elapsed:.3f}",
        )

    def cancel_all_created(self):
        with self.ids_lock:
            job_ids = list(self.created_ids.values())
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.concurrency) as executor:
            futures = [executor.submit(self.cancel_job, job_id) for job_id in job_ids]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def run_integration_tests(self):
        test_env = self.worker_env.copy()
        if self.args.database_url:
            test_env["DATABASE_URL"] = self.args.database_url
        result = subprocess.run(
            ["go", "test", "-run", "^TestIntegration", "-count=1", "-v", "."],
            cwd=self.root,
            env=test_env,
            text=True,
            capture_output=True,
        )
        for line in result.stdout.splitlines():
            self.log_line("stdout", f"go-test: {line}")
            match = re.search(r"recovery query took ([0-9.]+)ms", line)
            if match:
                self.lease_recovery_query_ms = float(match.group(1))
        for line in result.stderr.splitlines():
            self.log_line("stderr", f"go-test: {line}")
        if result.returncode != 0:
            self.record_error(f"integration tests failed with exit code {result.returncode}")
            return 0
        return sum(1 for line in result.stdout.splitlines() if line.startswith("--- PASS: TestIntegration"))

    def fetch_run_jobs(self):
        jobs = []
        offset = 0
        while True:
            status, page = self.call_api(
                f"{self.api_url}/jobs?limit=100&offset={offset}"
            )
            if status != 200 or not isinstance(page, list):
                raise RuntimeError(f"job listing returned HTTP {status}")
            run_jobs = [
                job
                for job in page
                if isinstance(job, dict)
                and isinstance(job.get("idempotency_key"), str)
                and job["idempotency_key"].startswith(self.run_prefix)
            ]
            jobs.extend(run_jobs)
            if len(run_jobs) >= self.args.jobs:
                return jobs
            if len(page) < 100:
                return jobs
            offset += len(page)

    def wait_for_completion(self):
        deadline = time.monotonic() + self.args.completion_timeout
        latest_jobs = []
        started = time.perf_counter()
        while time.monotonic() < deadline:
            latest_jobs = self.fetch_run_jobs()
            states = [job.get("status") for job in latest_jobs]
            if len(latest_jobs) >= self.args.jobs and all(state in TERMINAL_STATES for state in states):
                self.queue_drain_seconds = time.perf_counter() - started
                return latest_jobs
            time.sleep(2)
        self.queue_drain_seconds = time.perf_counter() - started
        return latest_jobs

    def stop_background_threads(self):
        self.stop_cancellations.set()
        self.stop_chaos.set()
        if self.cancellation_thread is not None:
            self.cancellation_thread.join()
        if self.chaos_thread is not None:
            self.chaos_thread.join()
        self.stop_metrics.set()
        if self.metrics_thread is not None:
            self.metrics_thread.join()

    def stop_workers(self):
        with self.worker_lock:
            processes = list(self.worker_processes)
        for process in processes:
            if process is None:
                continue
            try:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired) as exc:
                self.record_error(f"could not stop worker subprocess: {exc}")
        for index, stdout_path, stderr_path in self.worker_log_records:
            for channel, log_path in (("worker stdout", stdout_path), ("worker stderr", stderr_path)):
                if not log_path.exists():
                    continue
                with log_path.open(encoding="utf-8", errors="replace") as worker_log:
                    for line in worker_log:
                        if channel == "worker stdout" and "lease recovery requeued" in line:
                            self.worker_recovery_events += 1
                        self.log_line(channel, f"worker[{index + 1}] {line.rstrip()}")

    def summary(self, jobs):
        expected_keys = {f"{self.run_prefix}{i}" for i in range(self.args.jobs)}
        actual_keys = [
            job.get("idempotency_key")
            for job in jobs
            if isinstance(job.get("idempotency_key"), str)
        ]
        actual_key_set = set(actual_keys)
        missing_keys = expected_keys - actual_key_set
        extra_keys = actual_key_set - expected_keys
        status_counts = {}
        for job in jobs:
            status = str(job.get("status", "unknown"))
            status_counts[status] = status_counts.get(status, 0) + 1
        multiple_side_effects = sum(
            1 for job in jobs if int(job.get("side_effect_count", 0)) > 1
        )
        stuck_running = status_counts.get("running", 0)
        unfinished = status_counts.get("running", 0) + status_counts.get("queued", 0)
        orphaned = sum(
            1
            for job in jobs
            if job.get("status") not in TERMINAL_STATES
            or job.get("worker_id") is not None
            or job.get("lease_expires_at") is not None
        )
        elapsed = (
            self.load_finished_at - self.load_started_at
            if self.load_started_at is not None and self.load_finished_at is not None
            else 0
        )
        post_count = self.request_counts["POST /jobs"]
        observed_rps = post_count / elapsed if elapsed > 0 else 0
        latencies = sorted(self.post_latencies_ms)
        p99 = percentile(latencies, 0.99)
        performance = {
            "target_post_rps": self.args.post_rps,
            "observed_post_rps": round(observed_rps, 2),
            "target_rate_within_5_percent": (
                abs(observed_rps - self.args.post_rps) <= self.args.post_rps * 0.05
            ),
            "post_load_duration_seconds": round(elapsed, 3),
            "post_latency_ms": {
                "count": len(latencies),
                "p50": percentile(latencies, 0.50),
                "p95": percentile(latencies, 0.95),
                "p99": p99,
                "p99_under_200ms": p99 is not None and p99 < 200,
                "max": round(latencies[-1], 3) if latencies else None,
            },
            "request_error_rate": round(
                self.request_error_count / max(1, sum(self.request_counts.values())), 6
            ),
            "queue_drain_duration_seconds": (
                round(self.queue_drain_seconds, 3) if self.queue_drain_seconds is not None else None
            ),
        }

        with self.metrics_lock:
            samples = list(self.pool_samples)
        pool = summarize_pool_samples(samples)
        pool["no_connection_waits"] = pool.get("available", False) and pool.get("wait_count_delta") == 0
        return {
            "run_prefix": self.run_prefix,
            "expected_unique_jobs": self.args.jobs,
            "actual_run_jobs": len(jobs),
            "total_jobs": len(jobs),
            "missing_jobs": len(missing_keys),
            "unexpected_or_duplicate_keys": len(actual_keys) - len(actual_key_set) + len(extra_keys),
            "status_counts": status_counts,
            "side_effect_total": sum(int(job.get("side_effect_count", 0)) for job in jobs),
            "jobs_with_multiple_side_effects": multiple_side_effects,
            "jobs_stuck_running": stuck_running,
            "unfinished_jobs": unfinished,
            "orphaned_jobs": orphaned,
            "worker_processes_started": sum(self.worker_restarts),
            "worker_kills": self.worker_kills,
            "unexpected_worker_exits": self.unexpected_worker_exits,
            "worker_lease_recovery_events": self.worker_recovery_events,
            "forced_expiry_recovery_query_ms": self.lease_recovery_query_ms,
            "integration_checks_passed": self.integration_checks,
            "http_status_counts": dict(sorted(self.http_status_counts.items())),
            "request_counts": dict(sorted(self.request_counts.items())),
            "request_errors": self.error_count,
            "http_request_errors": self.request_error_count,
            "sample_errors": self.errors,
            "performance": performance,
            "database_pool": pool,
            "invariants": {
                "total_jobs_matches_expected": len(jobs) == self.args.jobs,
                "zero_jobs_lost": len(jobs) == self.args.jobs and not missing_keys and not extra_keys,
                "zero_jobs_with_multiple_side_effects": multiple_side_effects == 0,
                "zero_jobs_stuck_running": stuck_running == 0,
                "zero_orphaned_jobs": orphaned == 0,
                "at_least_one_worker_killed": self.worker_kills > 0,
            },
        }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default="http://127.0.0.1:8080")
    parser.add_argument("--database-url", help="loopback PostgreSQL URL for worker subprocesses")
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--post-rps", type=float, default=100)
    parser.add_argument("--request-timeout", type=int, default=30)
    parser.add_argument("--startup-timeout", type=int, default=30)
    parser.add_argument("--completion-timeout", type=int, default=300)
    args = parser.parse_args()
    if not 1 <= args.jobs <= 100_000:
        parser.error("--jobs must be between 1 and 100000")
    if not 1 <= args.workers <= 64:
        parser.error("--workers must be between 1 and 64")
    if not 1 <= args.concurrency <= 256:
        parser.error("--concurrency must be between 1 and 256")
    if not 0 < args.post_rps <= 1_000:
        parser.error("--post-rps must be greater than 0 and no more than 1000")

    parsed_api = urlparse(args.api_url)
    if parsed_api.scheme not in ("http", "https") or not parsed_api.hostname:
        parser.error("--api-url must be an HTTP(S) URL")
    try:
        require_loopback(parsed_api.hostname, "--api-url")
    except ValueError as exc:
        parser.error(str(exc))
    if parsed_api.username or parsed_api.password:
        parser.error("--api-url must not contain credentials")

    database_url = args.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        parser.error("--database-url or DATABASE_URL is required for integration checks")
    parsed_db = urlparse(database_url)
    if parsed_db.scheme not in ("postgres", "postgresql") or not parsed_db.hostname:
        parser.error("--database-url must be a PostgreSQL URL with a hostname")
    try:
        require_loopback(parsed_db.hostname, "--database-url")
    except ValueError as exc:
        parser.error(str(exc))
    args.database_url = database_url
    return args


def build_binary(root, binary_path):
    result = subprocess.run(
        ["go", "build", "-o", str(binary_path), "."],
        cwd=root,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"go build failed:\n{result.stdout}{result.stderr}")


def main():
    args = parse_args()
    root = Path(__file__).resolve().parent
    run_prefix = f"chaos-{uuid.uuid4().hex}-"

    with tempfile.TemporaryDirectory(prefix="soup-job-queue-chaos-") as temp_dir:
        binary = Path(temp_dir) / ("soup-job-queue.exe" if os.name == "nt" else "soup-job-queue")
        build_binary(root, binary)

        run = ChaosRun(args, root, binary, temp_dir, run_prefix)
        run.log_line(
            "stdout",
            f"run configuration jobs={args.jobs} workers={args.workers} "
            f"post_rps={args.post_rps:g} concurrency={args.concurrency} "
            "kill_interval_seconds=2",
        )
        jobs = []
        try:
            run.check_api()
            status, stats = run.call_api(f"{run.api_url}/debug/db-stats")
            if status != 200 or not isinstance(stats, dict):
                raise RuntimeError(
                    "database-pool telemetry endpoint unavailable; "
                    "set ENABLE_DB_STATS_ENDPOINT=true for the app"
                )
            run.integration_checks = run.run_integration_tests()
            if run.integration_checks != 6:
                raise RuntimeError(
                    f"expected six passing integration checks, got {run.integration_checks}"
                )
            for index in range(args.workers):
                run.start_worker(index)
            run.wait_for_workers()
            run.start_chaos()

            run.submit_duplicate_posts()
            run.stop_cancellations.set()
            if run.cancellation_thread is not None:
                run.cancellation_thread.join()
            run.cancel_all_created()
            jobs = run.wait_for_completion()
        except KeyboardInterrupt:
            run.record_error("test interrupted")
            jobs = run.fetch_run_jobs()
        except Exception as exc:
            run.record_error(str(exc))
            try:
                jobs = run.fetch_run_jobs()
            except Exception as fetch_error:
                run.record_error(f"could not fetch final run jobs: {fetch_error}")
        finally:
            run.stop_background_threads()
            run.stop_workers()

        summary = run.summary(jobs)
        for invariant, passed in summary["invariants"].items():
            run.log_line("stdout", f"ASSERT {'PASS' if passed else 'FAIL'} {invariant}")
        run.log_line(
            "stdout",
            "PERFORMANCE "
            + json.dumps(
                {
                    "metrics": summary["performance"],
                    "database_pool": summary["database_pool"],
                },
                sort_keys=True,
            ),
        )
        summary["passed"] = (
            all(summary["invariants"].values())
            and summary["unfinished_jobs"] == 0
            and run.error_count == 0
            and run.integration_checks == 6
        )
        rendered_summary = json.dumps(summary, indent=2, sort_keys=True)
        run.log_line("stdout", f"final_summary:\n{rendered_summary}")
        print(rendered_summary)
        return 0 if summary["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
