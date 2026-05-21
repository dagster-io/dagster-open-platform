"""Common Room webhook ingest Lambda.

Receives POST requests from Common Room, validates the shared-secret header,
and writes the raw JSON payload to S3 under the webhooks/ prefix so the
Dagster asset can pick it up via Snowflake external table.

Auth: Common Room sends the literal secret in `x-commonroom-webhook-secret`
(confirmed via their docs and sorted-pie's ingest-auth.ts). Constant-time
compare to prevent timing attacks.

S3 key pattern: webhooks/YYYY/MM/DD/<timestamp>-<request-id>.json
This matches the partition columns in the Snowflake external table:
  split_part(METADATA$FILENAME, '/', 2) → YYYY
  split_part(METADATA$FILENAME, '/', 3) → MM
  split_part(METADATA$FILENAME, '/', 4) → DD
"""

import hashlib
import hmac
import json
import os
from datetime import UTC, datetime

import boto3

_s3 = boto3.client("s3")
_BUCKET = os.environ["COMMON_ROOM_BUCKET"]
_SECRET = os.environ["COMMON_ROOM_WEBHOOK_SECRET"]


def _constant_time_equal(a: str, b: str) -> bool:
    """Timing-safe string compare."""
    return hmac.compare_digest(a.encode(), b.encode())


def _s3_key(request_id: str) -> str:
    now = datetime.now(UTC)
    ts = now.strftime("%H-%M-%S")
    date_path = now.strftime("%Y/%m/%d")
    return f"webhooks/{date_path}/{ts}-{request_id}.json"


def lambda_handler(event: dict, context: object) -> dict:
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}

    # Validate Common Room shared secret header.
    provided = headers.get("x-commonroom-webhook-secret", "")
    if not _SECRET:
        return {"statusCode": 503, "body": json.dumps({"error": "ingest_disabled"})}
    if not _constant_time_equal(provided, _SECRET):
        return {"statusCode": 401, "body": json.dumps({"error": "unauthorized"})}

    # Parse body.
    raw_body = event.get("body") or ""
    try:
        json.loads(raw_body)  # validate JSON; raises JSONDecodeError if malformed
    except json.JSONDecodeError:
        return {"statusCode": 400, "body": json.dumps({"error": "invalid_json"})}

    # Write to S3. Use the Lambda request ID as the unique file suffix.
    request_id = (
        getattr(context, "aws_request_id", None) or hashlib.md5(raw_body.encode()).hexdigest()
    )
    key = _s3_key(request_id)

    _s3.put_object(
        Bucket=_BUCKET,
        Key=key,
        Body=raw_body.encode(),
        ContentType="application/json",
    )

    return {"statusCode": 200, "body": json.dumps({"action": "accepted"})}
