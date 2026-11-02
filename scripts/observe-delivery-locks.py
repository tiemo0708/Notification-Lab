#!/usr/bin/env python3
"""Bounded, read-only aggregate observations of one logged Testcontainers MySQL.

Credentials stay inside the container. Never export SQL text, row/transaction IDs,
process lists, payloads, container IDs, or raw diagnostic error messages.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import time


QUERY = """
SHOW GLOBAL STATUS WHERE Variable_name IN (
 'Innodb_row_lock_current_waits', 'Innodb_row_lock_time',
 'Innodb_row_lock_time_max', 'Innodb_row_lock_waits');
SELECT 'data_lock_wait_edges', COUNT(*) FROM performance_schema.data_lock_waits;
"""
KEYS = {"Innodb_row_lock_current_waits", "Innodb_row_lock_time",
        "Innodb_row_lock_time_max", "Innodb_row_lock_waits", "data_lock_wait_edges"}
MYSQL = re.compile(r"Container mysql:8\.4\.8 is starting: ([0-9a-f]{64})")
PROGRESS = re.compile(r"DELIVERY_PILOT targets=(\d+) completed=(\d+) realtimeProbes=(\d+) elapsedSeconds=([\d.]+)")
LOGIN = 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --protocol=socket --user=root --batch --skip-column-names --connect-timeout=2 --execute "$1"'


def utc():
    return datetime.now(timezone.utc).isoformat()


def command(args):
    return subprocess.run(args, capture_output=True, text=True, timeout=8, check=False)


def validate_container(container):
    result = command(["docker", "inspect", "--format",
                      '{{.Config.Image}}|{{index .Config.Labels "org.testcontainers"}}', container])
    if result.returncode or result.stdout.strip() != "mysql:8.4.8|true":
        raise ValueError("Container identity validation failed")


def observe(container):
    result = command(["docker", "exec", container, "sh", "-c", LOGIN, "lock-observer", QUERY])
    if result.returncode:
        raise RuntimeError("Diagnostic query did not complete")
    values = {}
    for line in result.stdout.strip().splitlines():
        key, value = line.split("\t")
        if key not in KEYS or key in values:
            raise ValueError("Unexpected diagnostic field")
        values[key] = int(value)
        if values[key] < 0:
            raise ValueError("Negative counter")
    if values.keys() != KEYS:
        raise ValueError("Missing diagnostic field")
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--max-seconds", type=int, default=6000)
    args = parser.parse_args()
    if not 30 <= args.interval_seconds <= 120 or not 60 <= args.max_seconds <= 6000:
        parser.error("interval must be30..120 seconds; duration60..6000 seconds")
    if args.log.resolve() == args.output.resolve() or args.output.exists():
        parser.error("output must be new and different from the experiment log")
    report = {"observerVersion": "mysql-global-row-lock-and-wait-edges-v1", "startedAtUtc": utc(),
              "intervalSeconds": args.interval_seconds, "maxSeconds": args.max_seconds,
              "maxSamples": 128, "commandTimeoutSeconds": 8,
              "scope": "fresh logged Testcontainers MySQL only; global counters and instantaneous wait edges",
              "samples": [], "errors": []}

    def save():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        temporary.replace(args.output)

    start = time.monotonic()
    next_sample = 0
    container = None
    consecutive_errors = 0
    save()
    try:
        while time.monotonic() - start < args.max_seconds:
            log = args.log.read_text(errors="replace") if args.log.exists() else ""
            if "BUILD SUCCESSFUL" in log or "BUILD FAILED" in log:
                report["status"] = "EXPERIMENT_ENDED"
                report["buildOutcome"] = "SUCCESS" if "BUILD SUCCESSFUL" in log else "FAILURE"
                break
            if container is None and "tc.mysql:8.4.8 -- Container is started" in log:
                matches = MYSQL.findall(log)
                if len(matches) != 1:
                    raise ValueError("Expected exactly one experiment MySQL")
                container = matches[0]
                validate_container(container)
            if container is not None and time.monotonic() >= next_sample:
                if len(report["samples"]) + len(report["errors"]) >= 128:
                    report["status"] = "SAMPLE_BOUND_REACHED"
                    break
                progress = PROGRESS.findall(log)
                sample = {"observedAtUtc": utc(), "observerSeconds": time.monotonic() - start,
                          "phase": "DRAIN" if progress else "PRELOAD_OR_BASELINE"}
                if progress:
                    targets, completed, probes, elapsed = progress[-1]
                    sample.update({"targets": int(targets), "lastLoggedCompleted": int(completed),
                                   "lastLoggedRealtimeProbes": int(probes), "lastLoggedDrainSeconds": float(elapsed)})
                begin = time.monotonic()
                try:
                    sample["counters"] = observe(container)
                    sample["observationMillis"] = (time.monotonic() - begin) * 1000
                    report["samples"].append(sample)
                    consecutive_errors = 0
                    print(json.dumps(sample), flush=True)
                except (RuntimeError, ValueError, subprocess.TimeoutExpired) as failure:
                    # Raw stderr can contain connection/configuration information; keep only type.
                    report["errors"].append({"observedAtUtc": utc(), "errorType": type(failure).__name__})
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        report["status"] = "CONSECUTIVE_QUERY_ERRORS"
                        break
                next_sample = time.monotonic() + args.interval_seconds
                save()
            time.sleep(1)
        else:
            report["status"] = "DURATION_BOUND_REACHED"
    except (OSError, ValueError, subprocess.TimeoutExpired) as failure:
        report["status"] = "OBSERVER_ERROR"
        report["errorType"] = type(failure).__name__
    finally:
        report["finishedAtUtc"] = utc()
        save()
    return 0 if report.get("status") == "EXPERIMENT_ENDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
