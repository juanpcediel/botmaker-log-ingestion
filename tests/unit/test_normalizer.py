"""Unit tests for src/normalizer.py — field extraction and schema normalization.

Validates:
- All 16 fields produced with correct snake_case names and types
- Each extraction chain individually with specific fixtures
- get_nested with absent keys, null intermediates, non-dict intermediates, empty strings
- UUID v4 generation for ingestion_id
- ISO-8601 format for received_at
- schema_version is always 1
- raw_event_json compact serialization with ensure_ascii=False
- Missing required field rejection (environment not derivable)

Requirements: 2.1–2.7, 3.1–3.13, 4.2, 10.2
"""

import json
import os
import re
import uuid

import pytest

os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("FIREHOSE_STREAM_NAME", "test")
os.environ.setdefault("SECRET_ID", "test")

from normalizer import (
    NormalizationError,
    extract_event_id,
    extract_event_type,
    get_nested,
    normalize,
)


# ─── Constants ───────────────────────────────────────────────────────────────

EXPECTED_FIELDS = {
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

UUID_V4_REGEX = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

ISO_8601_MS_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
)


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def minimal_body():
    """Minimal body with eventType to ensure valid normalization."""
    return {"eventType": "MESSAGE_RECEIVED"}


@pytest.fixture
def full_body():
    """Body with all extractable fields populated."""
    return {
        "eventId": "evt-001",
        "eventType": "MESSAGE_RECEIVED",
        "messageId": "msg-001",
        "conversation": {
            "conversationId": "conv-001",
            "messageId": "conv-msg-001",
            "userId": "user-001",
        },
        "source": {
            "channel": "whatsapp",
            "bot": "bot-alpha",
        },
        "error": {
            "type": "TIMEOUT",
            "severity": "HIGH",
            "code": "E500",
        },
        "orderForm": {
            "id": "of-001",
        },
        "context": {
            "orderFormId": "ctx-of-001",
        },
    }


# ─── TestNormalizeOutput ─────────────────────────────────────────────────────

class TestNormalizeOutput:
    """Verify the 16-field normalized record structure, snake_case, and types."""

    def test_produces_exactly_16_fields(self, minimal_body):
        record = normalize(minimal_body, "req-123", "test")
        assert set(record.keys()) == EXPECTED_FIELDS

    def test_all_field_names_are_snake_case(self, minimal_body):
        record = normalize(minimal_body, "req-123", "test")
        for key in record.keys():
            assert key == key.lower(), f"Field '{key}' is not lowercase"
            assert " " not in key, f"Field '{key}' has a space"
            # snake_case: only letters, digits, underscores
            assert re.match(r"^[a-z][a-z0-9_]*$", key), f"'{key}' is not snake_case"

    def test_required_fields_are_non_null(self, minimal_body):
        record = normalize(minimal_body, "req-123", "test")
        required = [
            "ingestion_id",
            "event_id",
            "received_at",
            "schema_version",
            "environment",
            "event_type",
            "raw_event_json",
        ]
        for field in required:
            assert record[field] is not None, f"Required field '{field}' is None"

    def test_optional_fields_are_null_when_absent(self, minimal_body):
        record = normalize(minimal_body, "req-123", "test")
        optional = [
            "conversation_id",
            "user_id",
            "order_form_id",
            "source_channel",
            "source_bot",
            "severity",
            "error_code",
        ]
        for field in optional:
            assert record[field] is None, f"Optional field '{field}' should be None"

    def test_full_body_populates_all_optional_fields(self, full_body):
        record = normalize(full_body, "req-123", "test")
        assert record["conversation_id"] == "conv-001"
        assert record["message_id"] == "msg-001"
        assert record["user_id"] == "user-001"
        assert record["order_form_id"] == "of-001"
        assert record["source_channel"] == "whatsapp"
        assert record["source_bot"] == "bot-alpha"
        assert record["severity"] == "HIGH"
        assert record["error_code"] == "E500"

    def test_ingestion_id_is_valid_uuid_v4(self, minimal_body):
        record = normalize(minimal_body, "req-123", "test")
        assert UUID_V4_REGEX.match(record["ingestion_id"])
        # Verify it's parseable as UUID
        parsed = uuid.UUID(record["ingestion_id"])
        assert parsed.version == 4

    def test_received_at_is_iso8601_with_ms(self, minimal_body):
        record = normalize(minimal_body, "req-123", "test")
        assert ISO_8601_MS_REGEX.match(record["received_at"]), (
            f"received_at '{record['received_at']}' does not match ISO-8601 with ms"
        )

    def test_schema_version_is_always_1(self, minimal_body):
        record = normalize(minimal_body, "req-123", "test")
        assert record["schema_version"] == 1
        assert isinstance(record["schema_version"], int)

    def test_environment_is_passed_through(self, minimal_body):
        record = normalize(minimal_body, "req-123", "production")
        assert record["environment"] == "production"

    def test_request_id_is_passed_through(self, minimal_body):
        record = normalize(minimal_body, "req-abc-456", "test")
        assert record["request_id"] == "req-abc-456"

    def test_request_id_none_when_empty(self, minimal_body):
        record = normalize(minimal_body, "", "test")
        assert record["request_id"] is None

    def test_each_call_generates_unique_ingestion_id(self, minimal_body):
        r1 = normalize(minimal_body, "req-1", "test")
        r2 = normalize(minimal_body, "req-2", "test")
        assert r1["ingestion_id"] != r2["ingestion_id"]


# ─── TestExtractEventId ──────────────────────────────────────────────────────

class TestExtractEventId:
    """Test event_id extraction chain: eventId > errorId > linkId > messageId > conversation.messageId > UUID."""

    def test_eventId_is_first_priority(self):
        body = {"eventId": "evt-1", "errorId": "err-1", "linkId": "lnk-1"}
        assert extract_event_id(body) == "evt-1"

    def test_errorId_is_second_priority(self):
        body = {"errorId": "err-1", "linkId": "lnk-1", "messageId": "msg-1"}
        assert extract_event_id(body) == "err-1"

    def test_linkId_is_third_priority(self):
        body = {"linkId": "lnk-1", "messageId": "msg-1"}
        assert extract_event_id(body) == "lnk-1"

    def test_messageId_is_fourth_priority(self):
        body = {"messageId": "msg-1", "conversation": {"messageId": "conv-msg-1"}}
        assert extract_event_id(body) == "msg-1"

    def test_conversation_messageId_is_fifth_priority(self):
        body = {"conversation": {"messageId": "conv-msg-1"}}
        assert extract_event_id(body) == "conv-msg-1"

    def test_generates_uuid_v4_when_all_absent(self):
        body = {"someOtherField": "value"}
        result = extract_event_id(body)
        assert UUID_V4_REGEX.match(result)

    def test_skips_null_eventId(self):
        body = {"eventId": None, "errorId": "err-1"}
        assert extract_event_id(body) == "err-1"

    def test_skips_empty_string_eventId(self):
        body = {"eventId": "", "errorId": "err-1"}
        assert extract_event_id(body) == "err-1"

    def test_skips_whitespace_only_eventId(self):
        body = {"eventId": "   ", "errorId": "err-1"}
        assert extract_event_id(body) == "err-1"

    def test_skips_null_conversation_messageId(self):
        body = {"conversation": {"messageId": None}}
        result = extract_event_id(body)
        assert UUID_V4_REGEX.match(result)

    def test_skips_empty_conversation_messageId(self):
        body = {"conversation": {"messageId": ""}}
        result = extract_event_id(body)
        assert UUID_V4_REGEX.match(result)

    def test_empty_body_generates_uuid(self):
        result = extract_event_id({})
        assert UUID_V4_REGEX.match(result)


# ─── TestExtractEventType ────────────────────────────────────────────────────

class TestExtractEventType:
    """Test event_type extraction: eventType > error.type > type > PAYMENT_LINK_EVENT > ORDER_EVENT > UNKNOWN."""

    def test_eventType_is_first_priority(self):
        body = {"eventType": "MSG", "error": {"type": "TIMEOUT"}, "type": "fallback"}
        assert extract_event_type(body) == "MSG"

    def test_error_type_is_second_priority(self):
        body = {"error": {"type": "TIMEOUT"}, "type": "fallback"}
        assert extract_event_type(body) == "TIMEOUT"

    def test_type_is_third_priority(self):
        body = {"type": "CUSTOM_EVENT"}
        assert extract_event_type(body) == "CUSTOM_EVENT"

    def test_paymentLink_key_triggers_PAYMENT_LINK_EVENT(self):
        body = {"paymentLink": {"id": "pl-1"}}
        assert extract_event_type(body) == "PAYMENT_LINK_EVENT"

    def test_orders_key_triggers_ORDER_EVENT(self):
        body = {"orders": [{"id": "ord-1"}]}
        assert extract_event_type(body) == "ORDER_EVENT"

    def test_paymentLink_takes_priority_over_orders(self):
        body = {"paymentLink": {}, "orders": []}
        assert extract_event_type(body) == "PAYMENT_LINK_EVENT"

    def test_returns_UNKNOWN_when_nothing_matches(self):
        body = {"someRandomField": "value"}
        assert extract_event_type(body) == "UNKNOWN"

    def test_skips_null_eventType(self):
        body = {"eventType": None, "type": "FALLBACK"}
        assert extract_event_type(body) == "FALLBACK"

    def test_skips_empty_string_eventType(self):
        body = {"eventType": "", "type": "FALLBACK"}
        assert extract_event_type(body) == "FALLBACK"

    def test_skips_whitespace_only_eventType(self):
        body = {"eventType": "   ", "type": "FALLBACK"}
        assert extract_event_type(body) == "FALLBACK"

    def test_empty_body_returns_UNKNOWN(self):
        assert extract_event_type({}) == "UNKNOWN"

    def test_skips_null_error_type(self):
        body = {"error": {"type": None}, "type": "FALLBACK"}
        assert extract_event_type(body) == "FALLBACK"

    def test_skips_empty_error_type(self):
        body = {"error": {"type": ""}, "type": "FALLBACK"}
        assert extract_event_type(body) == "FALLBACK"


# ─── TestGetNested ───────────────────────────────────────────────────────────

class TestGetNested:
    """Test get_nested with absent keys, None intermediates, non-dict intermediates, empty strings."""

    def test_returns_value_for_valid_path(self):
        body = {"a": {"b": {"c": "value"}}}
        assert get_nested(body, "a", "b", "c") == "value"

    def test_returns_none_for_absent_key(self):
        body = {"a": {"b": "value"}}
        assert get_nested(body, "a", "x") is None

    def test_returns_none_for_absent_top_key(self):
        body = {"a": "value"}
        assert get_nested(body, "z") is None

    def test_returns_none_for_null_intermediate(self):
        body = {"a": None}
        assert get_nested(body, "a", "b") is None

    def test_returns_none_for_non_dict_intermediate(self):
        body = {"a": "not_a_dict"}
        assert get_nested(body, "a", "b") is None

    def test_returns_none_for_list_intermediate(self):
        body = {"a": [1, 2, 3]}
        assert get_nested(body, "a", "b") is None

    def test_returns_none_for_int_intermediate(self):
        body = {"a": 42}
        assert get_nested(body, "a", "b") is None

    def test_returns_none_for_empty_string_leaf(self):
        body = {"a": {"b": ""}}
        assert get_nested(body, "a", "b") is None

    def test_returns_none_for_whitespace_only_leaf(self):
        body = {"a": {"b": "   "}}
        assert get_nested(body, "a", "b") is None

    def test_returns_none_for_null_leaf(self):
        body = {"a": {"b": None}}
        assert get_nested(body, "a", "b") is None

    def test_returns_none_for_non_string_leaf(self):
        body = {"a": {"b": 123}}
        assert get_nested(body, "a", "b") is None

    def test_single_key_valid(self):
        body = {"key": "val"}
        assert get_nested(body, "key") == "val"

    def test_deep_path(self):
        body = {"a": {"b": {"c": {"d": "deep"}}}}
        assert get_nested(body, "a", "b", "c", "d") == "deep"

    @pytest.mark.parametrize("intermediate", [None, 42, "string", [], True])
    def test_returns_none_for_various_non_dict_intermediates(self, intermediate):
        body = {"a": intermediate}
        assert get_nested(body, "a", "b") is None

    def test_empty_body(self):
        assert get_nested({}, "a", "b") is None


# ─── TestFieldExtraction ─────────────────────────────────────────────────────

class TestFieldExtraction:
    """Test individual field extractions: conversation_id, message_id, user_id, order_form_id, source_channel, source_bot, severity, error_code."""

    def test_conversation_id_from_conversation_conversationId(self):
        body = {"eventType": "MSG", "conversation": {"conversationId": "conv-42"}}
        record = normalize(body, "req-1", "test")
        assert record["conversation_id"] == "conv-42"

    def test_conversation_id_null_when_absent(self):
        body = {"eventType": "MSG"}
        record = normalize(body, "req-1", "test")
        assert record["conversation_id"] is None

    def test_message_id_from_messageId(self):
        body = {"eventType": "MSG", "messageId": "msg-direct"}
        record = normalize(body, "req-1", "test")
        assert record["message_id"] == "msg-direct"

    def test_message_id_fallback_to_conversation_messageId(self):
        body = {"eventType": "MSG", "conversation": {"messageId": "conv-msg-1"}}
        record = normalize(body, "req-1", "test")
        assert record["message_id"] == "conv-msg-1"

    def test_message_id_prefers_messageId_over_conversation(self):
        body = {
            "eventType": "MSG",
            "messageId": "direct",
            "conversation": {"messageId": "nested"},
        }
        record = normalize(body, "req-1", "test")
        assert record["message_id"] == "direct"

    def test_message_id_null_when_all_absent(self):
        body = {"eventType": "MSG"}
        record = normalize(body, "req-1", "test")
        assert record["message_id"] is None

    def test_user_id_from_conversation_userId(self):
        body = {"eventType": "MSG", "conversation": {"userId": "usr-1"}}
        record = normalize(body, "req-1", "test")
        assert record["user_id"] == "usr-1"

    def test_user_id_null_when_absent(self):
        body = {"eventType": "MSG"}
        record = normalize(body, "req-1", "test")
        assert record["user_id"] is None

    def test_order_form_id_from_orderForm_id(self):
        body = {"eventType": "MSG", "orderForm": {"id": "of-primary"}}
        record = normalize(body, "req-1", "test")
        assert record["order_form_id"] == "of-primary"

    def test_order_form_id_fallback_to_context_orderFormId(self):
        body = {"eventType": "MSG", "context": {"orderFormId": "of-ctx"}}
        record = normalize(body, "req-1", "test")
        assert record["order_form_id"] == "of-ctx"

    def test_order_form_id_prefers_orderForm_over_context(self):
        body = {
            "eventType": "MSG",
            "orderForm": {"id": "of-primary"},
            "context": {"orderFormId": "of-secondary"},
        }
        record = normalize(body, "req-1", "test")
        assert record["order_form_id"] == "of-primary"

    def test_order_form_id_null_when_all_absent(self):
        body = {"eventType": "MSG"}
        record = normalize(body, "req-1", "test")
        assert record["order_form_id"] is None

    def test_source_channel_from_source_channel(self):
        body = {"eventType": "MSG", "source": {"channel": "telegram"}}
        record = normalize(body, "req-1", "test")
        assert record["source_channel"] == "telegram"

    def test_source_channel_null_when_absent(self):
        body = {"eventType": "MSG"}
        record = normalize(body, "req-1", "test")
        assert record["source_channel"] is None

    def test_source_bot_from_source_bot(self):
        body = {"eventType": "MSG", "source": {"bot": "my-bot"}}
        record = normalize(body, "req-1", "test")
        assert record["source_bot"] == "my-bot"

    def test_source_bot_null_when_absent(self):
        body = {"eventType": "MSG"}
        record = normalize(body, "req-1", "test")
        assert record["source_bot"] is None

    def test_severity_from_error_severity(self):
        body = {"eventType": "MSG", "error": {"severity": "CRITICAL"}}
        record = normalize(body, "req-1", "test")
        assert record["severity"] == "CRITICAL"

    def test_severity_null_when_absent(self):
        body = {"eventType": "MSG"}
        record = normalize(body, "req-1", "test")
        assert record["severity"] is None

    def test_error_code_from_error_code(self):
        body = {"eventType": "MSG", "error": {"code": "ERR_TIMEOUT"}}
        record = normalize(body, "req-1", "test")
        assert record["error_code"] == "ERR_TIMEOUT"

    def test_error_code_null_when_absent(self):
        body = {"eventType": "MSG"}
        record = normalize(body, "req-1", "test")
        assert record["error_code"] is None


# ─── TestRawEventJson ────────────────────────────────────────────────────────

class TestRawEventJson:
    """Test raw_event_json compact serialization with ensure_ascii=False."""

    def test_compact_serialization_no_spaces(self):
        body = {"eventType": "MSG", "key": "value"}
        record = normalize(body, "req-1", "test")
        raw = record["raw_event_json"]
        # Compact: no spaces after separators
        assert ": " not in raw
        assert ", " not in raw

    def test_round_trip_preserves_original_body(self):
        body = {"eventType": "MSG", "nested": {"a": 1, "b": [2, 3]}}
        record = normalize(body, "req-1", "test")
        deserialized = json.loads(record["raw_event_json"])
        assert deserialized == body

    def test_ensure_ascii_false_preserves_unicode(self):
        body = {"eventType": "MSG", "name": "Bogota\u0301"}
        record = normalize(body, "req-1", "test")
        raw = record["raw_event_json"]
        # Should NOT escape unicode to \uXXXX
        assert "Bogot\u00e1" in raw or "Bogota\u0301" in raw
        # Verify round-trip
        deserialized = json.loads(raw)
        assert deserialized == body

    def test_unicode_emoji_preserved(self):
        body = {"eventType": "MSG", "emoji": "\U0001f600"}
        record = normalize(body, "req-1", "test")
        raw = record["raw_event_json"]
        deserialized = json.loads(raw)
        assert deserialized["emoji"] == "\U0001f600"

    def test_empty_body_with_event_type(self):
        body = {"eventType": "MSG"}
        record = normalize(body, "req-1", "test")
        expected = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        assert record["raw_event_json"] == expected


# ─── TestNormalizationError ──────────────────────────────────────────────────

class TestNormalizationError:
    """Test NormalizationError exception for missing required fields."""

    def test_missing_environment_raises_normalization_error(self):
        body = {"eventType": "MSG"}
        with pytest.raises(NormalizationError) as exc_info:
            normalize(body, "req-1", "")
        assert exc_info.value.field == "environment"

    def test_none_environment_raises_normalization_error(self):
        body = {"eventType": "MSG"}
        with pytest.raises(NormalizationError) as exc_info:
            normalize(body, "req-1", None)
        assert exc_info.value.field == "environment"

    def test_whitespace_environment_raises_normalization_error(self):
        body = {"eventType": "MSG"}
        with pytest.raises(NormalizationError) as exc_info:
            normalize(body, "req-1", "   ")
        assert exc_info.value.field == "environment"

    def test_normalization_error_has_field_and_message(self):
        body = {"eventType": "MSG"}
        with pytest.raises(NormalizationError) as exc_info:
            normalize(body, "req-1", "")
        error = exc_info.value
        assert error.field == "environment"
        assert "environment" in error.message.lower() or "environment" in str(error)

    def test_normalization_error_is_exception(self):
        err = NormalizationError("test_field", "test message")
        assert isinstance(err, Exception)
        assert err.field == "test_field"
        assert err.message == "test message"

    def test_normalization_error_default_message(self):
        err = NormalizationError("my_field")
        assert "my_field" in err.message
