"""Lambda entry point and request routing for Botmaker log ingestion.

Orchestrates the request flow:
    auth → parse → validate → normalize → deliver → respond

Handles base64-encoded bodies, JSON validation, size limits,
correlation ID propagation, and structured logging at each stage.

Requirements: 1.2, 1.4, 1.5, 4.1, 4.3, 4.4, 5.12, 5.13, 9.2, 9.5
"""

from __future__ import annotations

import base64
import json
from typing import Any
from uuid import uuid4

import boto3

from src.auth import SecretCache, validate_token
from src.config import get_config, MAX_BODY_SIZE_BYTES, MAX_RAW_PAYLOAD_BYTES
from src.firehose_client import (
    FIREHOSE_BOTO_CONFIG,
    FirehoseDeliveryError,
    deliver_record,
)
from src.logger import (
    EVENT_TYPE_FIREHOSE_ACCEPTED,
    EVENT_TYPE_INGESTION_RECEIVED,
    EVENT_TYPE_INGESTION_VALIDATED,
    StructuredLogger,
    Timer,
)
from src.normalizer import NormalizationError, normalize


# --- Module-level resources (reused across warm invocations) ---

_secret_cache = SecretCache()

_sm_client = boto3.client("secretsmanager")

_firehose_client = boto3.client(
    "firehose",
    config=FIREHOSE_BOTO_CONFIG,
)


# --- Response helpers ---


def _response(status_code: int, body: dict) -> dict:
    """Build an API Gateway Lambda proxy response."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _error_400(message: str) -> dict:
    return _response(400, {"error": "Bad Request", "message": message})


def _error_413(message: str) -> dict:
    return _response(413, {"error": "Payload Too Large", "message": message})


def _error_503(message: str) -> dict:
    return _response(503, {"error": "Service Unavailable", "message": message})


# --- Main handler ---


def lambda_handler(event: dict, context: Any) -> dict:
    """AWS Lambda entry point for Botmaker webhook ingestion.

    Args:
        event: API Gateway HTTP API v2.0 Lambda proxy event.
        context: Lambda runtime context object.

    Returns:
        API Gateway Lambda proxy response dict.
    """
    config = get_config()

    # Generate correlation_id from API Gateway request ID
    correlation_id = event.get("requestContext", {}).get("requestId", str(uuid4()))

    logger = StructuredLogger(correlation_id, log_payload=config.log_payload)

    # --- Step 1: Authentication ---
    with Timer() as auth_timer:
        auth_valid, auth_error = validate_token(
            event=event,
            secret_cache=_secret_cache,
            sm_client=_sm_client,
            secret_id=config.secret_id,
            logger=logger,
        )

    if not auth_valid:
        return auth_error

    # --- Step 2: Extract and decode body ---
    raw_body = event.get("body", "")

    if raw_body is None:
        raw_body = ""

    # Check raw payload size (pre-decode) — 6 MB limit
    raw_size = len(raw_body.encode("utf-8")) if isinstance(raw_body, str) else len(raw_body)
    if raw_size > MAX_RAW_PAYLOAD_BYTES:
        logger.warning(
            EVENT_TYPE_INGESTION_RECEIVED,
            outcome="failure",
            error_message="Raw payload exceeds 6 MB limit",
        )
        return _error_413("Request body exceeds maximum allowed size")

    # Handle base64-encoded bodies
    if event.get("isBase64Encoded", False):
        try:
            decoded_bytes = base64.b64decode(raw_body)
            body_str = decoded_bytes.decode("utf-8")
        except Exception:
            logger.warning(
                EVENT_TYPE_INGESTION_RECEIVED,
                outcome="failure",
                error_message="Failed to decode base64 body",
            )
            return _error_400("Payload is not parseable JSON")
    else:
        body_str = raw_body

    # --- Step 3: Validate decoded body size (≤ 1 MB) ---
    decoded_size = len(body_str.encode("utf-8"))
    if decoded_size > MAX_BODY_SIZE_BYTES:
        logger.warning(
            EVENT_TYPE_INGESTION_RECEIVED,
            outcome="failure",
            error_message="Decoded body exceeds 1 MB limit",
        )
        return _error_400("Payload exceeds maximum allowed size")

    # --- Step 4: Parse JSON ---
    try:
        body = json.loads(body_str)
    except (json.JSONDecodeError, ValueError):
        logger.warning(
            EVENT_TYPE_INGESTION_RECEIVED,
            outcome="failure",
            error_message="Body is not valid JSON",
        )
        return _error_400("Payload is not parseable JSON")

    # --- Step 5: Validate body is a JSON object ---
    if not isinstance(body, dict):
        logger.warning(
            EVENT_TYPE_INGESTION_RECEIVED,
            outcome="failure",
            error_message="Body is not a JSON object",
        )
        return _error_400("Body is not a valid JSON object")

    # Log INGESTION_RECEIVED (successful receipt after auth + parse)
    logger.info(EVENT_TYPE_INGESTION_RECEIVED, outcome="success")

    # --- Step 6: Normalize ---
    try:
        record = normalize(
            body=body,
            request_id=correlation_id,
            environment=config.environment,
        )
    except NormalizationError as exc:
        logger.warning(
            EVENT_TYPE_INGESTION_VALIDATED,
            outcome="failure",
            error_message=exc.message,
        )
        return _error_400(f"Missing required field: {exc.field}")

    # Log INGESTION_VALIDATED
    logger.info(
        EVENT_TYPE_INGESTION_VALIDATED,
        outcome="success",
        ingestion_id=record["ingestion_id"],
        source_event_type=record["event_type"],
    )

    # --- Step 7: Deliver to Firehose ---
    with Timer() as delivery_timer:
        try:
            deliver_record(
                record=record,
                stream_name=config.firehose_stream_name,
                client=_firehose_client,
            )
        except FirehoseDeliveryError:
            logger.error(
                EVENT_TYPE_FIREHOSE_ACCEPTED,
                outcome="failure",
                ingestion_id=record["ingestion_id"],
                error_message="PutRecord failed after retries",
            )
            return _error_503("Unable to deliver record to storage")

    # Log FIREHOSE_ACCEPTED
    logger.info(
        EVENT_TYPE_FIREHOSE_ACCEPTED,
        outcome="success",
        ingestion_id=record["ingestion_id"],
        source_event_type=record["event_type"],
        duration_ms=delivery_timer.duration_ms,
    )

    # --- Step 8: Return success ---
    return _response(202, {"status": "accepted", "ingestion_id": record["ingestion_id"]})
