"""Check a small synthetic run through the API and HTTP Mock SSH forwards."""

import argparse
import http.client
import json
from pathlib import Path
import time
from datetime import datetime, timezone
from uuid import UUID, uuid4


PENDING_STATUSES = {"QUEUED", "PROCESSING", "WAITING_RETRY"}
MOCK_COUNTERS = ("requests", "accepted", "rateLimited", "serverErrors", "permanentFailures", "busy")
MAX_RESPONSE_BYTES = 64 * 1024


class SmokeFailure(Exception):
    pass


def require(condition, message):
    if not condition:
        raise SmokeFailure(message)


def request(port, path, expected, deadline, payload=None, key=None):
    remaining = deadline - time.monotonic()
    require(remaining > 0, "smoke deadline expired")
    headers = {"Accept": "application/json", "Connection": "close"}
    if payload is not None:
        headers.update({"Content-Type": "application/json", "Idempotency-Key": key})
    method = "POST" if payload is not None else "GET"
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=min(5, remaining))
    try:
        conn.request(method, path, body=json.dumps(payload) if payload is not None else None, headers=headers)
        with conn.getresponse() as response:
            require(response.status == expected, f"{method} returned {response.status}, expected {expected}")
            location = response.getheader("Location")
            content = bytearray()
            while True:
                remaining = deadline - time.monotonic()
                require(remaining > 0, "smoke deadline expired while reading a response")
                # The socket already has the request timeout. read1 lets us check the total budget between reads.
                block = response.read1(min(8192, MAX_RESPONSE_BYTES + 1 - len(content)))
                if not block:
                    break
                content.extend(block)
                require(len(content) <= MAX_RESPONSE_BYTES, "response exceeded the size limit")
            data = json.loads(content)
    except (OSError, http.client.HTTPException, ValueError) as exc:
        raise SmokeFailure(f"{method} response unavailable ({type(exc).__name__})") from exc
    finally:
        conn.close()
    require(isinstance(data, dict), "expected a JSON object")
    return data, location


def mock_stats(port, deadline):
    data, _ = request(port, "/mock/stats", 200, deadline)
    for name in (*MOCK_COUNTERS, "active"):
        require(type(data.get(name)) is int and data[name] >= 0, f"invalid Mock counter: {name}")
    return {name: data[name] for name in (*MOCK_COUNTERS, "active")}


def notification_id(data):
    value = data.get("notificationId")
    try:
        require(isinstance(value, str) and str(UUID(value)) == value, "invalid notification ID")
    except ValueError as exc:
        raise SmokeFailure("invalid notification ID") from exc
    return value


def smoke(args, report, deadline):
    port = args.api_port
    health, _ = request(port, "/actuator/health", 200, deadline)
    require(health.get("status") == "UP", "API health is not UP")
    config, _ = request(args.mock_port, "/mock/config", 200, deadline)
    require(config.get("mode") == "SUCCESS", "HTTP Mock must use SUCCESS mode")
    report["mockBefore"] = before = mock_stats(args.mock_port, deadline)
    require(before["active"] == 0, "HTTP Mock has an active request")

    run_id = report["runId"]
    submitted = set()
    for index in range(args.count):
        report["stage"] = "submit"
        report["attemptedSubmissions"] += 1
        key = f"remote-smoke-{run_id}-{index}"
        payload = {"recipientId": key, "title": "Synthetic smoke", "body": "Notification Lab synthetic check"}
        data, location = request(port, "/notifications", 202, deadline, payload, key)
        identifier = notification_id(data)
        require(identifier not in submitted, "new submissions returned the same ID")
        submitted.add(identifier)
        report["acceptedByApi"] += 1
        require(location == f"/notifications/{identifier}" and data.get("status") == "QUEUED",
                "new submission did not return a queued resource")
        if index == 0:
            report["stage"] = "replay"
            replay, replay_location = request(port, "/notifications", 200, deadline, payload, key)
            require(notification_id(replay) == identifier and replay_location == location,
                    "idempotent replay returned a different resource")
            report["replayVerified"] = True
            report["stage"] = "conflict"
            conflicting = dict(payload, body="Different synthetic body")
            request(port, "/notifications", 409, deadline, conflicting, key)
            report["conflictVerified"] = True

    report["stage"] = "verify_delivery"
    pending = submitted.copy()
    provider_ids = set()
    while pending:
        for identifier in tuple(pending):
            path = f"/notifications/{identifier}"
            result, _ = request(port, path, 200, deadline)
            require(notification_id(result) == identifier, "lookup returned a different resource")
            status = result.get("status")
            if status in PENDING_STATUSES:
                continue
            require(status == "PROVIDER_ACCEPTED", "notification ended without provider acceptance")
            provider_id = result.get("providerMessageId")
            require(isinstance(provider_id, str) and provider_id.startswith("http-mock-")
                    and provider_id not in provider_ids, "HTTP Mock message ID is absent or repeated")
            delivery, _ = request(port, path + "/delivery", 200, deadline)
            attempts = delivery.get("attempts")
            require(delivery.get("status") == status and delivery.get("attemptCount") == 1
                    and delivery.get("nextAttemptAt") is None and isinstance(attempts, list) and len(attempts) == 1,
                    "expected one completed delivery attempt")
            attempt = attempts[0]
            require(isinstance(attempt, dict) and attempt.get("attemptNo") == 1
                    and attempt.get("outcome") == status and attempt.get("completedAt"),
                    "stored attempt does not match the completed delivery")
            provider_ids.add(provider_id)
            pending.remove(identifier)
            report["persistedAccepted"] += 1
        if pending:
            time.sleep(max(0, min(0.25, deadline - time.monotonic())))

    report["stage"] = "verify_mock"
    after = mock_stats(args.mock_port, deadline)
    report["mockAfter"] = after
    report["mockDelta"] = delta = {name: after[name] - before[name] for name in MOCK_COUNTERS}
    expected = dict.fromkeys(MOCK_COUNTERS, 0)
    expected.update(requests=args.count, accepted=args.count)
    require(delta == expected and after["active"] == 0, "HTTP Mock totals do not match the completed deliveries")
    report.update(result="PASS", stage="done")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-port", type=int, default=18000)
    parser.add_argument("--mock-port", type=int, default=18001)
    parser.add_argument("--count", type=int, choices=range(1, 101), metavar="1..100", default=20)
    parser.add_argument("--report", type=Path, required=True, help="new JSON path; never overwritten")
    args = parser.parse_args()
    if not all(1 <= port <= 65535 for port in (args.api_port, args.mock_port)) or args.api_port == args.mock_port:
        parser.error("API and Mock must use distinct, valid ports")

    try:
        output = args.report.open("x", encoding="utf-8")
    except OSError as exc:
        parser.error(f"cannot create report: {exc}")

    started = time.monotonic()
    # The pilot sends serially, leaving extra time for 100 Mock responses over the tunnel.
    timeout = 60 if args.count <= 20 else 120
    report = {
        "runId": uuid4().hex,
        "startedAtUtc": datetime.now(timezone.utc).isoformat(),
        "result": "FAIL",
        "stage": "preflight",
        "requestedCount": args.count,
        "attemptedSubmissions": 0,
        "acceptedByApi": 0,
        "persistedAccepted": 0,
        "replayVerified": False,
        "conflictVerified": False,
        "timeoutSeconds": timeout,
    }
    with output:
        try:
            smoke(args, report, started + timeout)
        except SmokeFailure as exc:
            report["reason"] = str(exc)
        except KeyboardInterrupt:
            report["reason"] = "interrupted"
        finally:
            report["elapsedSeconds"] = round(time.monotonic() - started, 3)
            json.dump(report, output, indent=2)
            output.write("\n")
    if report["result"] != "PASS":
        print(f"FAIL ({report['stage']}): {report.get('reason', 'unexpected error')}; see {args.report}")
        return 1
    print(f"PASS: {args.count} deliveries matched; see {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
