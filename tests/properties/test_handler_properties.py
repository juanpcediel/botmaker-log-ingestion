"""Property-based tests for the handler module.

Properties tested:
  - Property 5: Base64 Decoding Equivalence
  - Property 6: Non-Object JSON Rejection
  - Property 9: Log Redaction Safety

Minimum 100 Hypothesis examples per property.
"""

from __future__ import annotations

import base64
import json
import os

os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("FIREHOSE_STREAM_NAME", "test-stream")
os.environ.setdefault("SECRET_ID", "test-secret")

from unittest.mock import patch, MagicMock

from hypothesis import given, settings, assume
from hypothesis import strategies as st

from src.config import get_config

# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Strategy for webhook-like JSON objects (valid bodies)
webhook_objects = st.fixed_dictionaries(
    {"eventId": st.text(min_size=1, max_size=32).filter(lambda s: s.strip() != "")},
    optional={
        "eventType": st.text(min_size=1, max_size=32).filter(lambda s: s.strip() != ""),
        "messageId": st.text(min_size=1, max_size=32),
        "conversation": st.fixed_dictionaries({}, optional={
            "conversationId": st.text(max_size=32),
            "messageId": st.text(max_size=32),
            "userId": st.text(max_size=32),
        }),
        "source": st.fixed_dictionaries({}, optional={
            "channel": st.text(max_size=16),
            "bot": st.text(max_size=16),
        }),
    },
)

# Non-object JSON values (arrays, strings, numbers, booleans, null)
non_object_json_values = st.one_of(
    st.lists(st.integers(), max_size=5),
    st.lists(st.text(max_size=10), max_size=3),
    st.text(min_size=0, max_size=64),
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.booleans(),
    st.just(None),
)

# Strategy for auth tokens (non-empty ASCII strings)
auth_tokens = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P", "S"), min_codepoint=33, max_codepoint=126),
    min_size=8,
    max_size=64,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_event(body_str: str, is_base64: bool = False, token: str = "valid-token-123") -> dict:
    """Build a minimal valid API Gateway v2.0 event."""
    return {
        "requestContext": {
            "requestId": "test-req-id",
            "http": {"sourceIp": "127.0.0.1"},
        },
        "headers": {"authorization": f"Bearer {token}"},
        "body": body_str,
        "isBase64Encoded": is_base64,
    }


def _patch_handler_dependencies():
    """Return a context manager that patches handler's external deps."""
    mock_cache = MagicMock()
    mock_cache.get.return_value = "valid-token-123"

    mock_firehose = MagicMock()
    mock_firehose.put_record.return_value = {"RecordId": "rec-001"}

    mock_sm = MagicMock()
    mock_sm.get_secret_value.return_value = {"SecretString": "valid-token-123"}

    return (
        patch("src.handler._secret_cache", mock_cache),
        patch("src.handler._firehose_client", mock_firehose),
        patch("src.handler._sm_client", mock_sm),
    )


# ---------------------------------------------------------------------------
# Property 5: Base64 Decoding Equivalence
# ---------------------------------------------------------------------------


@settings(max_examples=100, deadline=10000)
@given(body=webhook_objects)
def test_base64_decoding_equivalence(body):
    """Processing a valid JSON object both raw and base64-encoded produces
    equivalent Normalized_Records (excluding received_at and ingestion_id).

    **Validates: Requirements 4.1**
    """
    get_config.cache_clear()

    from src.handler import lambda_handler

    body_str = json.dumps(body, ensure_ascii=False)

    # Ensure body is not oversized
    assume(len(body_str.encode("utf-8")) <= 1_000_000)

    # Process raw
    event_raw = _build_event(body_str, is_base64=False)

    # Process base64
    b64_body = base64.b64encode(body_str.encode("utf-8")).decode("ascii")
    event_b64 = _build_event(b64_body, is_base64=True)

    p1, p2, p3 = _patch_handler_dependencies()
    with p1, p2 as mock_fh_raw, p3:
        response_raw = lambda_handler(event_raw, None)

    p4, p5, p6 = _patch_handler_dependencies()
    with p4, p5 as mock_fh_b64, p6:
        response_b64 = lambda_handler(event_b64, None)

    # Both should return 202
    assert response_raw["statusCode"] == 202, f"Raw returned {response_raw['statusCode']}"
    assert response_b64["statusCode"] == 202, f"Base64 returned {response_b64['statusCode']}"

    # Extract records from firehose calls
    raw_call = mock_fh_raw.put_record.call_args[1]
    b64_call = mock_fh_b64.put_record.call_args[1]

    raw_record = json.loads(raw_call["Record"]["Data"].decode("utf-8"))
    b64_record = json.loads(b64_call["Record"]["Data"].decode("utf-8"))

    # Compare all fields except received_at and ingestion_id (generated fresh each time)
    exclude_keys = {"received_at", "ingestion_id"}
    for key in raw_record:
        if key not in exclude_keys:
            assert raw_record[key] == b64_record[key], (
                f"Field '{key}' differs: raw={raw_record[key]!r}, b64={b64_record[key]!r}"
            )


# ---------------------------------------------------------------------------
# Property 6: Non-Object JSON Rejection
# ---------------------------------------------------------------------------


@settings(max_examples=100, deadline=10000)
@given(value=non_object_json_values)
def test_non_object_json_rejected_with_400(value):
    """Any valid JSON value that is NOT an object (array, string, number,
    boolean, null) results in HTTP 400.

    **Validates: Requirements 4.3**
    """
    get_config.cache_clear()

    from src.handler import lambda_handler

    body_str = json.dumps(value)

    event = _build_event(body_str, is_base64=False)

    p1, p2, p3 = _patch_handler_dependencies()
    with p1, p2 as mock_fh, p3:
        response = lambda_handler(event, None)

    assert response["statusCode"] == 400, (
        f"Expected 400 for value {value!r}, got {response['statusCode']}"
    )
    # Firehose should NOT be called for rejected requests
    mock_fh.put_record.assert_not_called()


# ---------------------------------------------------------------------------
# Property 9: Log Redaction Safety
# ---------------------------------------------------------------------------


# Internal log constants that tokens might accidentally match as substrings.
# We filter these out to avoid false positives in the property test.
_INTERNAL_LOG_WORDS = frozenset({
    "INGESTION_RECEIVED", "INGESTION_VALIDATED", "FIREHOSE_ACCEPTED",
    "FIREHOSE", "INGESTION", "ACCEPTED", "RECEIVED", "VALIDATED",
    "timestamp", "correlation_id", "event_type", "outcome", "success",
    "failure", "ingestion_id", "source_event_type", "duration_ms",
    "INFO", "WARNING", "ERROR", "DEBUG",
})


@settings(max_examples=100, deadline=10000)
@given(token=auth_tokens, body=webhook_objects)
def test_log_entries_never_contain_literal_token(token, body):
    """No log entry emitted during request processing contains the literal
    auth token value.

    **Validates: Requirements 9.4, 16.2**
    """
    get_config.cache_clear()

    from src.handler import lambda_handler

    # Filter tokens that are too short or are substrings of internal log constants.
    # These would create false positives — the token isn't leaked, it just happens
    # to match part of an event name like "FIREHOSE_ACCEPTED".
    assume(len(token) >= 12)
    assume(not any(token in word or word in token for word in _INTERNAL_LOG_WORDS))

    body_str = json.dumps(body, ensure_ascii=False)
    assume(len(body_str.encode("utf-8")) <= 1_000_000)
    # Also exclude tokens that happen to be substrings of the serialized body
    # (since raw_event_json is NOT logged, but event_type derived from body might be)
    assume(token not in body_str)

    event = _build_event(body_str, is_base64=False, token=token)

    # Patch dependencies, use the same token as the expected secret
    mock_cache = MagicMock()
    mock_cache.get.return_value = token

    mock_firehose = MagicMock()
    mock_firehose.put_record.return_value = {"RecordId": "rec-001"}

    mock_sm = MagicMock()

    captured_logs: list[str] = []

    with (
        patch("src.handler._secret_cache", mock_cache),
        patch("src.handler._firehose_client", mock_firehose),
        patch("src.handler._sm_client", mock_sm),
        patch("builtins.print", side_effect=lambda msg, **kwargs: captured_logs.append(str(msg))),
    ):
        lambda_handler(event, None)

    # Verify no log entry contains the literal token
    for log_line in captured_logs:
        assert token not in log_line, (
            f"Token '{token}' found in log entry: {log_line[:200]}"
        )
