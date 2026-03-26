import base64
import json
import logging
import os
from datetime import UTC, datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

EVENT_BUCKET_NAME = os.getenv("EVENT_BUCKET_NAME", "").strip()
ALLOWED_EVENT_TYPES = {"BUY", "SELL", "DEPOSIT", "WITHDRAWAL"}


class EventValidationError(ValueError):
    pass


def lambda_handler(event: dict, context: object) -> dict:
    if not EVENT_BUCKET_NAME:
        logger.error("EVENT_BUCKET_NAME is not configured")
        return response(500, {"message": "Lambda misconfigured: missing EVENT_BUCKET_NAME"})

    try:
        payload = parse_request_body(event)
        transaction_event = validate_transaction_event(payload)
    except EventValidationError as error:
        logger.warning("Transaction event validation failed: %s", error)
        return response(
            400,
            {
                "message": "Invalid transaction event payload",
                "error": str(error),
            },
        )
    except json.JSONDecodeError as error:
        logger.warning("Unable to parse JSON payload: %s", error)
        return response(
            400,
            {
                "message": "Request body must be valid JSON",
                "error": str(error),
            },
        )

    event_key = build_s3_key(transaction_event["timestamp"], transaction_event["event_id"])

    try:
        s3_client.put_object(
            Bucket=EVENT_BUCKET_NAME,
            Key=event_key,
            Body=json.dumps(transaction_event).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception:
        logger.exception("Failed to persist event_id=%s to S3", transaction_event["event_id"])
        return response(
            500,
            {
                "message": "Unable to persist transaction event",
                "event_id": transaction_event["event_id"],
            },
        )

    logger.info(
        "Accepted transaction event event_id=%s bucket=%s key=%s",
        transaction_event["event_id"],
        EVENT_BUCKET_NAME,
        event_key,
    )

    return response(
        200,
        {
            "message": "Transaction event accepted",
            "event_id": transaction_event["event_id"],
            "bucket": EVENT_BUCKET_NAME,
            "key": event_key,
        },
    )


def parse_request_body(event: dict) -> dict:
    body = event.get("body")
    if body is None:
        raise EventValidationError("body is required")

    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    if not isinstance(body, str):
        raise EventValidationError("body must be a JSON string")

    payload = json.loads(body)
    if not isinstance(payload, dict):
        raise EventValidationError("payload must be a JSON object")

    return payload


def validate_transaction_event(payload: dict) -> dict[str, object]:
    event_id = require_non_empty_string(payload, "event_id")
    client_id = require_non_empty_string(payload, "client_id")
    asset = require_non_empty_string(payload, "asset").upper()
    event_type = require_non_empty_string(payload, "event_type").upper()
    timestamp = normalize_timestamp(require_non_empty_string(payload, "timestamp"))

    if event_type not in ALLOWED_EVENT_TYPES:
        raise EventValidationError(f"event_type must be one of {sorted(ALLOWED_EVENT_TYPES)}")

    amount = require_positive_number(payload, "amount", allow_missing=True)
    quantity = require_positive_integer(payload, "quantity", allow_missing=True)
    unit_price = require_positive_number(payload, "unit_price", allow_missing=True)
    notional_amount = require_positive_number(payload, "notional_amount", allow_missing=True)

    if quantity is None:
        quantity = 1

    if unit_price is None and amount is not None:
        unit_price = round(amount / quantity, 4)

    if notional_amount is None and unit_price is not None:
        notional_amount = round(unit_price * quantity, 2)

    if amount is None and notional_amount is not None:
        amount = notional_amount

    if amount is None or unit_price is None or notional_amount is None:
        raise EventValidationError(
            "payload must include amount or enough financial fields to derive it"
        )

    return {
        "event_id": event_id,
        "client_id": client_id,
        "asset": asset,
        "event_type": event_type,
        "unit_price": round(unit_price, 4),
        "quantity": quantity,
        "notional_amount": round(notional_amount, 2),
        "amount": round(notional_amount, 2),
        "timestamp": timestamp,
    }


def require_non_empty_string(payload: dict, field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def require_positive_number(
    payload: dict,
    field_name: str,
    allow_missing: bool = False,
) -> float | None:
    value = payload.get(field_name)
    if value is None and allow_missing:
        return None

    if not isinstance(value, (int, float)) or value <= 0:
        raise EventValidationError(f"{field_name} must be a positive number")

    return float(value)


def require_positive_integer(
    payload: dict,
    field_name: str,
    allow_missing: bool = False,
) -> int | None:
    value = payload.get(field_name)
    if value is None and allow_missing:
        return None

    if not isinstance(value, int) or value <= 0:
        raise EventValidationError(f"{field_name} must be a positive integer")

    return value


def normalize_timestamp(value: str) -> str:
    if "T" not in value:
        raise EventValidationError("timestamp must be an ISO 8601 datetime string")

    normalized_value = value.replace("Z", "+00:00")

    try:
        timestamp = datetime.fromisoformat(normalized_value)
    except ValueError as error:
        raise EventValidationError("timestamp must be a valid ISO 8601 datetime string") from error

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)

    return timestamp.isoformat()


def build_s3_key(timestamp_value: str, event_id: str) -> str:
    timestamp = datetime.fromisoformat(timestamp_value)
    return (
        f"events/{timestamp.strftime('%Y/%m/%d')}/{event_id}.json"
    )


def response(status_code: int, payload: dict[str, object]) -> dict[str, object]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
        },
        "body": json.dumps(payload),
    }
