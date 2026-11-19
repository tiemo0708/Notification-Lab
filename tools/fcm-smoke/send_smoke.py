"""Send one synthetic data message using FCM HTTP v1."""

import argparse
from datetime import datetime, timezone
from functools import partial
from http.client import HTTPException, HTTPSConnection
import json
from pathlib import Path
import re
from uuid import UUID, uuid4


HOST = "fcm.googleapis.com"
SCOPE = "https://www.googleapis.com/auth/firebase.messaging"
PROJECT = re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]")
DEVICE_TOKEN = re.compile(rb"[A-Za-z0-9:_-]{32,4096}")
MESSAGE_NAME = re.compile(r"projects/([a-z0-9-]{1,30})/messages/[a-zA-Z0-9:%._-]{1,240}")
MAX_RESPONSE = 8192


def read_token(path):
    with path.open("rb") as token_file:
        content = token_file.read(4099)
    token = content.strip()
    if len(content) > 4098 or not DEVICE_TOKEN.fullmatch(token):
        raise ValueError("Invalid FCM registration token file.")
    return token.decode("ascii")


def access_token():
    import google.auth
    from google.auth.transport.requests import Request

    credentials, _ = google.auth.default(scopes=[SCOPE])
    credentials.refresh(partial(Request(), timeout=10))
    if not credentials.token:
        raise RuntimeError("No OAuth access token available.")
    return credentials.token


def submit(project, device_token, oauth_token, smoke_id, send=False, connect=HTTPSConnection):
    if not PROJECT.fullmatch(project):
        raise ValueError("Invalid Firebase project ID.")
    report = {"smokeId": str(smoke_id), "startedAt": datetime.now(timezone.utc).isoformat(),
              "mode": "send" if send else "validate_only"}
    payload = {"validate_only": not send,
               "message": {"token": device_token, "data": {"smokeId": str(smoke_id)}}}
    connection = connect(HOST, timeout=10)
    try:
        connection.request("POST", f"/v1/projects/{project}/messages:send", json.dumps(payload),
                           {"Authorization": f"Bearer {oauth_token}", "Content-Type": "application/json"})
        with connection.getresponse() as response:
            report["httpStatus"] = response.status
            if response.status != 200:
                report["result"] = "rejected" if 400 <= response.status < 500 and response.status != 408 else "unknown"
                return report
            body = response.read(MAX_RESPONSE + 1)
        if len(body) <= MAX_RESPONSE:
            document = json.loads(body)
            name = document.get("name", "") if isinstance(document, dict) else ""
            match = MESSAGE_NAME.fullmatch(name) if isinstance(name, str) else None
            if match and (match.group(1) == project or match.group(1).isdigit()):
                report.update(messageName=name, result="accepted" if send else "validated")
                return report
    except (HTTPException, OSError, ValueError):
        pass  # Transport and decoding exceptions can include credential or response contents.
    finally:
        connection.close()
    report["result"] = "unknown"
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="Firebase project ID")
    parser.add_argument("--token-file", required=True, type=Path, help="Local, private device token file")
    parser.add_argument("--smoke-id", type=UUID, default=None, help="Synthetic correlation UUID")
    parser.add_argument("--send", action="store_true", help="Send once. The default validates the request without delivery.")
    args = parser.parse_args()
    if not PROJECT.fullmatch(args.project):
        parser.error("Invalid Firebase project ID.")
    try:
        device = read_token(args.token_file)
    except (OSError, ValueError):
        parser.exit(2, "Could not read a valid FCM registration token file.\n")
    try:
        oauth = access_token()
    except Exception:
        parser.exit(2, "Could not obtain an OAuth access token. Check local Google ADC and the sender's permissions.\n")
    report = submit(args.project, device, oauth, args.smoke_id or uuid4(), send=args.send)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["result"] in {"accepted", "validated"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
