"""Property-based tests for the normalization module.

Properties tested:
  - Property 1: Normalization Invariant (16-field structure)
  - Property 2: Fallback Chain Priority
  - Property 3: Path Traversal Null Safety
  - Property 4: Raw Event JSON Round-Trip

Minimum 100 Hypothesis examples per property.
"""

import os

os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("FIREHOSE_STREAM_NAME", "test")
os.environ.setdefault("SECRET_ID", "test")

import json
import re
import uuid

from hypothesis import given, settings
from hypothesis import strategies as st

from src.normalizer import normalize

# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

webhook_payloads = st.fixed_dictionaries({}, optional={
    "eventId": st.text(min_size=1, max_size=64),
    "errorId": st.text(min_size=1, max_size=64),
    "linkId": st.text(min_size=1, max_size=64),
    "messageId": st.text(min_size=1, max_size=64),
    "eventType": st.text(min_size=1, max_size=64),
    "type": st.text(min_size=1, max_size=64),
    "paymentLink": st.fixed_dictionaries({}),
    "orders": st.lists(st.fixed_dictionaries({}), max_size=3),
    "conversation": st.fixed_dictionaries({}, optional={
        "conversationId": st.text(max_size=64),
        "messageId": st.text(max_size=64),
        "userId": st.text(max_size=64),
    }),
    "source": st.fixed_dictionaries({}, optional={
        "channel": st.text(max_size=32),
        "bot": st.text(max_size=32),
    }),
    "error": st.fixed_dictionaries({}, optional={
        "type": st.text(max_size=32),
        "severity": st.text(max_size=16),
        "code": st.text(max_size=32),
    }),
    "orderForm": st.fixed_dictionaries({}, optional={
        "id": st.text(max_size=64),
    }),
    "context": st.fixed_dictionaries({}, optional={
        "orderFormId": st.text(max_size=64),
    }),
})

# Expected 16 field names in the Normalized_Record
EXPECTED_KEYS = {
    "ingestion_id",
    "event_id",
    "request_id",
    "received_at",
    "schema_version",
    "environment",
    "event_type",
    "conversation_id",
    "message_id",
    "user_id",
    "order_form_id",
    "source_channel",
    "source_bot",
    "severity",
    "error_code",
    "raw_event_json",
}

REQUIRED_FIELDS = {
    "ingestion_id",
    "event_id",
    "received_at",
    "schema_version",
    "environment",
    "event_type",
    "raw_event_json",
}

UUID4_REGEX = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

ISO8601_MS_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
)

SNAKE_CASE_REGEX = re.compile(r"^[a-z][a-z0-9_]*$")


# ---------------------------------------------------------------------------
# Property 1: Normalization Invariant (16-field structure)
# ---------------------------------------------------------------------------


@settings(max_examples=100, deadline=5000)
@given(body=webhook_payloads)
def test_normalization_invariant_16_fields(body):
    """Verify exactly 16 keys, all snake_case, required fields non-null,
    ingestion_id is UUID v4, received_at is ISO-8601 ms, schema_version == 1.

    **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6**
    """
    record = normalize(body, request_id="req-123", environment="test")

    # Exactly 16 keys
    assert set(record.keys()) == EXPECTED_KEYS, (
        f"Expected 16 keys, got {len(record.keys())}: {set(record.keys()) ^ EXPECTED_KEYS}"
    )

    # All keys are snake_case
    for key in record.keys():
        assert SNAKE_CASE_REGEX.match(key), f"Key not snake_case: {key}"

    # Required fields are non-null
    for field in REQUIRED_FIELDS:
        assert record[field] is not None, f"Required field is null: {field}"

    # ingestion_id is a valid UUID v4
    assert UUID4_REGEX.match(record["ingestion_id"]), (
        f"ingestion_id not UUID v4: {record['ingestion_id']}"
    )

    # received_at is ISO-8601 with ms precision
    assert ISO8601_MS_REGEX.match(record["received_at"]), (
        f"received_at not ISO-8601 ms: {record['received_at']}"
    )

    # schema_version is always 1
    assert record["schema_version"] == 1


# ---------------------------------------------------------------------------
# Property 2: Fallback Chain Priority
# ---------------------------------------------------------------------------

# Strategy: generate payloads with explicit non-empty values for multiple
# candidates in each fallback chain so we can verify priority ordering.

non_empty_text = st.text(min_size=1, max_size=32).filter(lambda s: s.strip() != "")


@settings(max_examples=100, deadline=5000)
@given(
    event_id_val=non_empty_text,
    error_id_val=non_empty_text,
    link_id_val=non_empty_text,
    msg_id_val=non_empty_text,
    conv_msg_id_val=non_empty_text,
)
def test_event_id_fallback_priority(
    event_id_val, error_id_val, link_id_val, msg_id_val, conv_msg_id_val
):
    """event_id chain: eventId > errorId > linkId > messageId > conversation.messageId.

    **Validates: Requirements 3.1, 3.2**
    """
    # All candidates present — eventId wins
    body_all = {
        "eventId": event_id_val,
        "errorId": error_id_val,
        "linkId": link_id_val,
        "messageId": msg_id_val,
        "conversation": {"messageId": conv_msg_id_val},
    }
    record = normalize(body_all, request_id="r", environment="test")
    assert record["event_id"] == event_id_val

    # Remove eventId — errorId wins
    body_no_event = {
        "errorId": error_id_val,
        "linkId": link_id_val,
        "messageId": msg_id_val,
        "conversation": {"messageId": conv_msg_id_val},
    }
    record2 = normalize(body_no_event, request_id="r", environment="test")
    assert record2["event_id"] == error_id_val

    # Remove errorId too — linkId wins
    body_no_error = {
        "linkId": link_id_val,
        "messageId": msg_id_val,
        "conversation": {"messageId": conv_msg_id_val},
    }
    record3 = normalize(body_no_error, request_id="r", environment="test")
    assert record3["event_id"] == link_id_val

    # Remove linkId — messageId wins
    body_no_link = {
        "messageId": msg_id_val,
        "conversation": {"messageId": conv_msg_id_val},
    }
    record4 = normalize(body_no_link, request_id="r", environment="test")
    assert record4["event_id"] == msg_id_val

    # Remove messageId — conversation.messageId wins
    body_conv_only = {
        "conversation": {"messageId": conv_msg_id_val},
    }
    record5 = normalize(body_conv_only, request_id="r", environment="test")
    assert record5["event_id"] == conv_msg_id_val


@settings(max_examples=100, deadline=5000)
@given(
    event_type_val=non_empty_text,
    error_type_val=non_empty_text,
    type_val=non_empty_text,
)
def test_event_type_fallback_priority(event_type_val, error_type_val, type_val):
    """event_type chain: eventType > error.type > type > PAYMENT_LINK_EVENT > ORDER_EVENT > UNKNOWN.

    **Validates: Requirements 3.4**
    """
    # All present — eventType wins
    body_all = {
        "eventType": event_type_val,
        "error": {"type": error_type_val},
        "type": type_val,
        "paymentLink": {},
        "orders": [],
    }
    record = normalize(body_all, request_id="r", environment="test")
    assert record["event_type"] == event_type_val

    # Remove eventType — error.type wins
    body_no_et = {
        "error": {"type": error_type_val},
        "type": type_val,
        "paymentLink": {},
        "orders": [],
    }
    record2 = normalize(body_no_et, request_id="r", environment="test")
    assert record2["event_type"] == error_type_val

    # Remove error.type — type wins
    body_no_err = {
        "type": type_val,
        "paymentLink": {},
        "orders": [],
    }
    record3 = normalize(body_no_err, request_id="r", environment="test")
    assert record3["event_type"] == type_val

    # Remove type — paymentLink presence wins
    body_payment = {
        "paymentLink": {},
        "orders": [],
    }
    record4 = normalize(body_payment, request_id="r", environment="test")
    assert record4["event_type"] == "PAYMENT_LINK_EVENT"

    # Remove paymentLink — orders presence wins
    body_orders = {
        "orders": [],
    }
    record5 = normalize(body_orders, request_id="r", environment="test")
    assert record5["event_type"] == "ORDER_EVENT"

    # Nothing — UNKNOWN
    body_empty = {}
    record6 = normalize(body_empty, request_id="r", environment="test")
    assert record6["event_type"] == "UNKNOWN"


@settings(max_examples=100, deadline=5000)
@given(
    msg_id_val=non_empty_text,
    conv_msg_id_val=non_empty_text,
)
def test_message_id_fallback_priority(msg_id_val, conv_msg_id_val):
    """message_id chain: messageId > conversation.messageId.

    **Validates: Requirements 3.6**
    """
    body_both = {
        "messageId": msg_id_val,
        "conversation": {"messageId": conv_msg_id_val},
    }
    record = normalize(body_both, request_id="r", environment="test")
    assert record["message_id"] == msg_id_val

    body_conv_only = {
        "conversation": {"messageId": conv_msg_id_val},
    }
    record2 = normalize(body_conv_only, request_id="r", environment="test")
    assert record2["message_id"] == conv_msg_id_val


@settings(max_examples=100, deadline=5000)
@given(
    order_form_val=non_empty_text,
    context_val=non_empty_text,
)
def test_order_form_id_fallback_priority(order_form_val, context_val):
    """order_form_id chain: orderForm.id > context.orderFormId.

    **Validates: Requirements 3.6**
    """
    body_both = {
        "orderForm": {"id": order_form_val},
        "context": {"orderFormId": context_val},
    }
    record = normalize(body_both, request_id="r", environment="test")
    assert record["order_form_id"] == order_form_val

    body_ctx_only = {
        "context": {"orderFormId": context_val},
    }
    record2 = normalize(body_ctx_only, request_id="r", environment="test")
    assert record2["order_form_id"] == context_val


# ---------------------------------------------------------------------------
# Property 3: Path Traversal Null Safety
# ---------------------------------------------------------------------------

# Strategy: generate broken intermediate values for nested paths

broken_intermediates = st.one_of(
    st.none(),
    st.integers(),
    st.text(max_size=10),
    st.lists(st.none(), max_size=2),
    st.booleans(),
)


@settings(max_examples=100, deadline=5000)
@given(broken=broken_intermediates)
def test_conversation_id_null_with_broken_path(broken):
    """conversation.conversationId returns null when conversation is broken.

    **Validates: Requirements 3.3, 3.7**
    """
    body = {"conversation": broken}
    record = normalize(body, request_id="r", environment="test")
    assert record["conversation_id"] is None


@settings(max_examples=100, deadline=5000)
@given(broken=broken_intermediates)
def test_user_id_null_with_broken_path(broken):
    """conversation.userId returns null when conversation is broken.

    **Validates: Requirements 3.5, 3.8**
    """
    body = {"conversation": broken}
    record = normalize(body, request_id="r", environment="test")
    assert record["user_id"] is None


@settings(max_examples=100, deadline=5000)
@given(broken=broken_intermediates)
def test_source_channel_null_with_broken_path(broken):
    """source.channel returns null when source is broken.

    **Validates: Requirements 3.9, 3.10**
    """
    body = {"source": broken}
    record = normalize(body, request_id="r", environment="test")
    assert record["source_channel"] is None


@settings(max_examples=100, deadline=5000)
@given(broken=broken_intermediates)
def test_source_bot_null_with_broken_path(broken):
    """source.bot returns null when source is broken.

    **Validates: Requirements 3.9, 3.10**
    """
    body = {"source": broken}
    record = normalize(body, request_id="r", environment="test")
    assert record["source_bot"] is None


@settings(max_examples=100, deadline=5000)
@given(broken=broken_intermediates)
def test_severity_null_with_broken_path(broken):
    """error.severity returns null when error is broken.

    **Validates: Requirements 3.11, 3.12**
    """
    body = {"error": broken}
    record = normalize(body, request_id="r", environment="test")
    assert record["severity"] is None


@settings(max_examples=100, deadline=5000)
@given(broken=broken_intermediates)
def test_error_code_null_with_broken_path(broken):
    """error.code returns null when error is broken.

    **Validates: Requirements 3.11, 3.13**
    """
    body = {"error": broken}
    record = normalize(body, request_id="r", environment="test")
    assert record["error_code"] is None


@settings(max_examples=100, deadline=5000)
@given(broken=broken_intermediates)
def test_order_form_id_null_with_broken_paths(broken):
    """orderForm.id and context.orderFormId return null when both paths are broken.

    **Validates: Requirements 3.5**
    """
    body = {"orderForm": broken, "context": broken}
    record = normalize(body, request_id="r", environment="test")
    assert record["order_form_id"] is None


@settings(max_examples=100, deadline=5000)
@given(body=webhook_payloads)
def test_absent_keys_produce_null(body):
    """Fields that have no keys at all in the payload produce null.

    **Validates: Requirements 3.3, 3.5, 3.7, 3.8, 3.9, 3.10, 3.11, 3.12, 3.13**
    """
    # Remove all nested structures to test absent-key behavior
    stripped = {k: v for k, v in body.items() if k not in (
        "conversation", "source", "error", "orderForm", "context"
    )}
    record = normalize(stripped, request_id="r", environment="test")

    assert record["conversation_id"] is None
    assert record["user_id"] is None
    assert record["source_channel"] is None
    assert record["source_bot"] is None
    assert record["severity"] is None
    assert record["error_code"] is None
    assert record["order_form_id"] is None


# ---------------------------------------------------------------------------
# Property 4: Raw Event JSON Round-Trip
# ---------------------------------------------------------------------------


@settings(max_examples=100, deadline=5000)
@given(body=webhook_payloads)
def test_raw_event_json_round_trip(body):
    """json.loads(record['raw_event_json']) == original body.

    **Validates: Requirements 4.2**
    """
    record = normalize(body, request_id="r", environment="test")
    deserialized = json.loads(record["raw_event_json"])
    assert deserialized == body


# ---------------------------------------------------------------------------
# Property 8: Missing Required Field Rejection
# ---------------------------------------------------------------------------


@settings(max_examples=100, deadline=5000)
@given(body=st.fixed_dictionaries({}))
def test_missing_environment_raises_normalization_error(body):
    """When environment is empty/None, NormalizationError is raised with field='environment'.

    **Validates: Requirements 2.7**
    """
    from src.normalizer import NormalizationError
    import pytest

    with pytest.raises(NormalizationError) as exc_info:
        normalize(body, request_id="r", environment="")
    assert exc_info.value.field == "environment"
