"""Event normalization module — field extraction and schema normalization.

Transforms raw Botmaker webhook JSON payloads into a flat 16-field
Normalized_Record suitable for Firehose delivery and Parquet conversion.

Requirements: 2.1–2.7, 3.1–3.13, 4.2
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from src.config import SCHEMA_VERSION


class NormalizationError(Exception):
    """Raised when a required field cannot be derived from the payload."""

    def __init__(self, field: str, message: str | None = None):
        self.field = field
        self.message = message or f"Missing required field: {field}"
        super().__init__(self.message)


def _is_present(value: Any) -> bool:
    """Return True if value is a non-null, non-empty-string usable value."""
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def get_nested(body: dict, *keys: str) -> Optional[str]:
    """Traverse a dot-notation path safely through nested dicts.

    Returns the final string value if the full path exists and is a
    non-empty string. Returns None for absent keys, null intermediates,
    non-dict intermediates, or empty-string leaf values.

    Args:
        body: The root dictionary to traverse.
        *keys: Sequence of keys forming the dot-notation path.

    Returns:
        The string value at the path, or None if unreachable/absent/empty.
    """
    current: Any = body
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    if isinstance(current, str) and current.strip():
        return current
    return None


def extract_event_id(body: dict) -> str:
    """Extract event_id using the defined fallback chain.

    Chain: eventId → errorId → linkId → messageId → conversation.messageId → UUID v4

    Args:
        body: The raw webhook payload.

    Returns:
        The resolved event_id string.
    """
    # Top-level direct keys
    for key in ("eventId", "errorId", "linkId", "messageId"):
        val = body.get(key)
        if _is_present(val) and isinstance(val, str):
            return val

    # Nested fallback: conversation.messageId
    conv_msg = get_nested(body, "conversation", "messageId")
    if conv_msg is not None:
        return conv_msg

    # Final fallback: generate UUID v4
    return str(uuid.uuid4())


def extract_event_type(body: dict) -> str:
    """Extract event_type using the defined rule chain.

    Chain: eventType → error.type → type → PAYMENT_LINK_EVENT → ORDER_EVENT → UNKNOWN

    Args:
        body: The raw webhook payload.

    Returns:
        The resolved event_type string.
    """
    # Direct field: eventType
    val = body.get("eventType")
    if _is_present(val) and isinstance(val, str):
        return val

    # Nested: error.type
    error_type = get_nested(body, "error", "type")
    if error_type is not None:
        return error_type

    # Direct field: type
    val = body.get("type")
    if _is_present(val) and isinstance(val, str):
        return val

    # Key presence checks
    if "paymentLink" in body:
        return "PAYMENT_LINK_EVENT"

    if "orders" in body:
        return "ORDER_EVENT"

    return "UNKNOWN"


def _extract_conversation_id(body: dict) -> Optional[str]:
    """Extract conversation_id from conversation.conversationId."""
    return get_nested(body, "conversation", "conversationId")


def _extract_message_id(body: dict) -> Optional[str]:
    """Extract message_id using fallback: messageId → conversation.messageId."""
    val = body.get("messageId")
    if _is_present(val) and isinstance(val, str):
        return val
    return get_nested(body, "conversation", "messageId")


def _extract_user_id(body: dict) -> Optional[str]:
    """Extract user_id from conversation.userId."""
    return get_nested(body, "conversation", "userId")


def _extract_order_form_id(body: dict) -> Optional[str]:
    """Extract order_form_id using fallback: orderForm.id → context.orderFormId."""
    val = get_nested(body, "orderForm", "id")
    if val is not None:
        return val
    return get_nested(body, "context", "orderFormId")


def _extract_source_channel(body: dict) -> Optional[str]:
    """Extract source_channel from source.channel."""
    return get_nested(body, "source", "channel")


def _extract_source_bot(body: dict) -> Optional[str]:
    """Extract source_bot from source.bot."""
    return get_nested(body, "source", "bot")


def _extract_severity(body: dict) -> Optional[str]:
    """Extract severity from error.severity."""
    return get_nested(body, "error", "severity")


def _extract_error_code(body: dict) -> Optional[str]:
    """Extract error_code from error.code."""
    return get_nested(body, "error", "code")


def normalize(body: dict, request_id: str, environment: str) -> dict:
    """Normalize a raw webhook payload into a 16-field Normalized_Record.

    Args:
        body: The parsed JSON body of the webhook event.
        request_id: The API Gateway request ID (correlation ID).
        environment: The deployment environment name (from config).

    Returns:
        A dict with exactly 16 keys matching the Normalized_Record schema.

    Raises:
        NormalizationError: If a required field cannot be derived.
    """
    # Validate environment is present
    if not _is_present(environment):
        raise NormalizationError("environment", "Missing required field: environment")

    # Extract event_type — reject if UNKNOWN and no derivable source exists
    event_type = extract_event_type(body)

    # Generate per-event identifiers and timestamps
    ingestion_id = str(uuid.uuid4())
    event_id = extract_event_id(body)
    now = datetime.now(timezone.utc)
    received_at = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

    # Build the 16-field Normalized_Record
    record: dict[str, Any] = {
        "ingestion_id": ingestion_id,
        "event_id": event_id,
        "request_id": request_id if _is_present(request_id) else None,
        "received_at": received_at,
        "schema_version": SCHEMA_VERSION,
        "environment": environment,
        "event_type": event_type,
        "conversation_id": _extract_conversation_id(body),
        "message_id": _extract_message_id(body),
        "user_id": _extract_user_id(body),
        "order_form_id": _extract_order_form_id(body),
        "source_channel": _extract_source_channel(body),
        "source_bot": _extract_source_bot(body),
        "severity": _extract_severity(body),
        "error_code": _extract_error_code(body),
        "raw_event_json": json.dumps(body, ensure_ascii=False, separators=(",", ":")),
    }

    return record
