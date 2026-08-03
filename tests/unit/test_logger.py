"""Unit tests for src/logger.py — structured JSON logging with redaction.

Validates:
- Structured log entries contain required fields
- Sensitive values are recursively redacted
- Raw request bodies are never logged
- Event types are properly supported
- LOG_PAYLOAD config controls metadata logging
"""

import json
import time

import pytest

from src.logger import (
    EVENT_TYPE_FIREHOSE_ACCEPTED,
    EVENT_TYPE_INGESTION_RECEIVED,
    EVENT_TYPE_INGESTION_VALIDATED,
    StructuredLogger,
    Timer,
    redact_sensitive,
)


class TestRedactSensitive:
    """Tests for the redact_sensitive function."""

    def test_redacts_authorization_key(self):
        data = {"Authorization": "Bearer abc123"}
        result = redact_sensitive(data)
        assert result["Authorization"] == "[REDACTED]"

    def test_redacts_token_key(self):
        data = {"access_token": "secret-value-123"}
        result = redact_sensitive(data)
        assert result["access_token"] == "[REDACTED]"

    def test_redacts_secret_key(self):
        data = {"my_secret": "sensitive"}
        result = redact_sensitive(data)
        assert result["my_secret"] == "[REDACTED]"

    def test_redacts_password_key(self):
        data = {"password": "p@ssw0rd", "username": "admin"}
        result = redact_sensitive(data)
        assert result["password"] == "[REDACTED]"
        assert result["username"] == "admin"

    def test_redacts_api_key_variations(self):
        data = {
            "api_key": "key1",
            "api-key": "key2",
            "apikey": "key3",
        }
        result = redact_sensitive(data)
        assert result["api_key"] == "[REDACTED]"
        assert result["api-key"] == "[REDACTED]"
        assert result["apikey"] == "[REDACTED]"

    def test_redacts_bearer_key(self):
        data = {"bearer": "some-token"}
        result = redact_sensitive(data)
        assert result["bearer"] == "[REDACTED]"

    def test_redacts_credential_key(self):
        data = {"credential": "cred-value"}
        result = redact_sensitive(data)
        assert result["credential"] == "[REDACTED]"

    def test_case_insensitive_matching(self):
        data = {
            "AUTHORIZATION": "val1",
            "Token": "val2",
            "SECRET_KEY": "val3",
            "Password": "val4",
        }
        result = redact_sensitive(data)
        assert result["AUTHORIZATION"] == "[REDACTED]"
        assert result["Token"] == "[REDACTED]"
        assert result["SECRET_KEY"] == "[REDACTED]"
        assert result["Password"] == "[REDACTED]"

    def test_recursive_dict_redaction(self):
        data = {
            "headers": {
                "Authorization": "Bearer xyz",
                "Content-Type": "application/json",
            },
            "name": "test",
        }
        result = redact_sensitive(data)
        assert result["headers"]["Authorization"] == "[REDACTED]"
        assert result["headers"]["Content-Type"] == "application/json"
        assert result["name"] == "test"

    def test_recursive_list_redaction(self):
        data = [
            {"token": "secret1", "id": "1"},
            {"token": "secret2", "id": "2"},
        ]
        result = redact_sensitive(data)
        assert result[0]["token"] == "[REDACTED]"
        assert result[0]["id"] == "1"
        assert result[1]["token"] == "[REDACTED]"
        assert result[1]["id"] == "2"

    def test_deeply_nested_redaction(self):
        data = {
            "level1": {
                "level2": {
                    "level3": {
                        "api_key": "deep-secret",
                        "value": "safe",
                    }
                }
            }
        }
        result = redact_sensitive(data)
        assert result["level1"]["level2"]["level3"]["api_key"] == "[REDACTED]"
        assert result["level1"]["level2"]["level3"]["value"] == "safe"

    def test_does_not_mutate_input(self):
        original = {"Authorization": "Bearer token123", "data": {"secret": "val"}}
        original_copy = json.loads(json.dumps(original))
        redact_sensitive(original)
        assert original == original_copy

    def test_handles_non_string_keys(self):
        data = {123: "numeric_key", "token": "secret"}
        result = redact_sensitive(data)
        assert result[123] == "numeric_key"
        assert result["token"] == "[REDACTED]"

    def test_handles_scalar_values(self):
        assert redact_sensitive("hello") == "hello"
        assert redact_sensitive(42) == 42
        assert redact_sensitive(None) is None
        assert redact_sensitive(True) is True

    def test_handles_empty_structures(self):
        assert redact_sensitive({}) == {}
        assert redact_sensitive([]) == []

    def test_mixed_list_and_dict(self):
        data = {
            "items": [
                {"name": "item1", "secret_key": "s1"},
                "plain_string",
                42,
                {"password": "pass"},
            ]
        }
        result = redact_sensitive(data)
        assert result["items"][0]["secret_key"] == "[REDACTED]"
        assert result["items"][0]["name"] == "item1"
        assert result["items"][1] == "plain_string"
        assert result["items"][2] == 42
        assert result["items"][3]["password"] == "[REDACTED]"


class TestStructuredLogger:
    """Tests for the StructuredLogger class."""

    def test_log_entry_has_required_fields(self, capsys):
        logger = StructuredLogger(correlation_id="req-123")
        logger.info(EVENT_TYPE_INGESTION_RECEIVED)

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)

        assert "timestamp" in entry
        assert entry["level"] == "INFO"
        assert entry["correlation_id"] == "req-123"
        assert entry["event_type"] == "INGESTION_RECEIVED"
        assert entry["outcome"] == "success"

    def test_timestamp_is_iso8601_utc(self, capsys):
        logger = StructuredLogger(correlation_id="req-456")
        logger.info(EVENT_TYPE_INGESTION_VALIDATED)

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)

        ts = entry["timestamp"]
        assert ts.endswith("Z")
        # Should parse without error
        from datetime import datetime
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        assert parsed.tzinfo is not None

    def test_timestamp_has_millisecond_precision(self, capsys):
        logger = StructuredLogger(correlation_id="req-789")
        logger.info(EVENT_TYPE_INGESTION_RECEIVED)

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)

        ts = entry["timestamp"]
        # Format: 2024-01-15T09:30:00.123Z — 3 digits of ms
        # Split at dot, check right side is 3 digits + Z
        parts = ts.split(".")
        assert len(parts) == 2
        assert len(parts[1]) == 4  # "123Z"
        assert parts[1].endswith("Z")

    def test_correlation_id_consistent_across_calls(self, capsys):
        logger = StructuredLogger(correlation_id="consistent-id")
        logger.info(EVENT_TYPE_INGESTION_RECEIVED)
        logger.info(EVENT_TYPE_INGESTION_VALIDATED)
        logger.info(EVENT_TYPE_FIREHOSE_ACCEPTED)

        lines = capsys.readouterr().out.strip().split("\n")
        for line in lines:
            entry = json.loads(line)
            assert entry["correlation_id"] == "consistent-id"

    def test_optional_fields_included_when_provided(self, capsys):
        logger = StructuredLogger(correlation_id="req-opt")
        logger.info(
            EVENT_TYPE_INGESTION_VALIDATED,
            ingestion_id="uuid-123",
            source_event_type="MESSAGE_RECEIVED",
            duration_ms=15.5,
        )

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)

        assert entry["ingestion_id"] == "uuid-123"
        assert entry["source_event_type"] == "MESSAGE_RECEIVED"
        assert entry["duration_ms"] == 15.5

    def test_optional_fields_omitted_when_none(self, capsys):
        logger = StructuredLogger(correlation_id="req-min")
        logger.info(EVENT_TYPE_INGESTION_RECEIVED)

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)

        assert "ingestion_id" not in entry
        assert "source_event_type" not in entry
        assert "duration_ms" not in entry
        assert "error_message" not in entry

    def test_error_message_included(self, capsys):
        logger = StructuredLogger(correlation_id="req-err")
        logger.error(
            EVENT_TYPE_FIREHOSE_ACCEPTED,
            error_message="PutRecord failed after retries",
        )

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)

        assert entry["level"] == "ERROR"
        assert entry["outcome"] == "failure"
        assert entry["error_message"] == "PutRecord failed after retries"

    def test_warning_level(self, capsys):
        logger = StructuredLogger(correlation_id="req-warn")
        logger.warning(EVENT_TYPE_INGESTION_RECEIVED)

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)
        assert entry["level"] == "WARNING"
        assert entry["outcome"] == "failure"

    def test_log_payload_false_no_metadata(self, capsys):
        logger = StructuredLogger(correlation_id="req-no-payload", log_payload=False)
        logger.info(
            EVENT_TYPE_INGESTION_VALIDATED,
            payload_metadata={"event_type": "MSG", "fields_present": ["eventId"]},
        )

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)
        assert "payload_metadata" not in entry

    def test_log_payload_true_includes_metadata(self, capsys):
        logger = StructuredLogger(correlation_id="req-with-payload", log_payload=True)
        logger.info(
            EVENT_TYPE_INGESTION_VALIDATED,
            payload_metadata={"event_type": "MSG", "fields_present": ["eventId"]},
        )

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)
        assert "payload_metadata" in entry
        assert entry["payload_metadata"]["event_type"] == "MSG"

    def test_payload_metadata_is_redacted(self, capsys):
        logger = StructuredLogger(correlation_id="req-meta-redact", log_payload=True)
        logger.info(
            EVENT_TYPE_INGESTION_VALIDATED,
            payload_metadata={
                "event_type": "MSG",
                "authorization": "Bearer leaked-token",
            },
        )

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)
        assert entry["payload_metadata"]["authorization"] == "[REDACTED]"
        assert entry["payload_metadata"]["event_type"] == "MSG"

    def test_never_logs_raw_request_body(self, capsys):
        """Ensure the logger never includes raw body content."""
        logger = StructuredLogger(correlation_id="req-body-safe", log_payload=True)
        # Even if someone passes body content in metadata, sensitive fields are redacted
        logger.info(
            EVENT_TYPE_INGESTION_RECEIVED,
            payload_metadata={"event_type": "TEST", "fields_present": ["a", "b"]},
        )

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)
        # Only metadata about the payload (event_type, field presence), never raw content
        assert "payload_metadata" in entry
        assert "body" not in entry

    def test_sensitive_values_never_appear_in_logs(self, capsys):
        """Verify the final safety pass catches any leaked secrets."""
        logger = StructuredLogger(correlation_id="req-safety", log_payload=True)
        # Simulate a scenario where secret_token ends up in metadata
        logger.info(
            EVENT_TYPE_INGESTION_RECEIVED,
            payload_metadata={"secret_token": "abc-super-secret"},
        )

        output = capsys.readouterr().out.strip()
        assert "abc-super-secret" not in output

    def test_returns_json_string(self):
        logger = StructuredLogger(correlation_id="req-ret")
        result = logger.info(EVENT_TYPE_INGESTION_RECEIVED)

        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed["correlation_id"] == "req-ret"

    def test_json_default_str_serialization(self, capsys):
        """json.dumps with default=str handles non-serializable types."""
        from datetime import datetime, timezone

        logger = StructuredLogger(correlation_id="req-ser", log_payload=True)
        logger.info(
            EVENT_TYPE_INGESTION_VALIDATED,
            payload_metadata={"timestamp": datetime(2024, 1, 15, tzinfo=timezone.utc)},
        )

        output = capsys.readouterr().out.strip()
        # Should not raise, datetime is serialized via default=str
        entry = json.loads(output)
        assert "2024-01-15" in entry["payload_metadata"]["timestamp"]

    def test_all_event_types_supported(self, capsys):
        logger = StructuredLogger(correlation_id="req-events")

        for event_type in [
            EVENT_TYPE_INGESTION_RECEIVED,
            EVENT_TYPE_INGESTION_VALIDATED,
            EVENT_TYPE_FIREHOSE_ACCEPTED,
        ]:
            logger.info(event_type)

        lines = capsys.readouterr().out.strip().split("\n")
        event_types = [json.loads(line)["event_type"] for line in lines]
        assert event_types == [
            "INGESTION_RECEIVED",
            "INGESTION_VALIDATED",
            "FIREHOSE_ACCEPTED",
        ]

    def test_correlation_id_property(self):
        logger = StructuredLogger(correlation_id="my-corr-id")
        assert logger.correlation_id == "my-corr-id"


class TestTimer:
    """Tests for the Timer context manager."""

    def test_measures_duration(self):
        with Timer() as t:
            time.sleep(0.01)

        assert t.duration_ms >= 5  # At least ~10ms but allow for timing variance
        assert t.duration_ms < 500  # Shouldn't take half a second

    def test_duration_ms_is_float(self):
        with Timer() as t:
            pass

        assert isinstance(t.duration_ms, float)

    def test_integrates_with_logger(self, capsys):
        logger = StructuredLogger(correlation_id="req-timer")

        with Timer() as t:
            time.sleep(0.01)

        logger.info(
            EVENT_TYPE_FIREHOSE_ACCEPTED,
            duration_ms=t.duration_ms,
        )

        output = capsys.readouterr().out.strip()
        entry = json.loads(output)
        assert "duration_ms" in entry
        assert entry["duration_ms"] >= 5
