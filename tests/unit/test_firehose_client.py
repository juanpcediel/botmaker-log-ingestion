"""Unit tests for src/firehose_client.py — Firehose PutRecord wrapper.

Validates:
- Successful PutRecord returns RecordId
- Firehose unavailable raises FirehoseDeliveryError
- Timeout raises FirehoseDeliveryError
- Permission errors raise FirehoseDeliveryError
- SDK adaptive retry configuration

Requirements: 5.1, 5.12, 5.13, 10.2
"""

import json
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from src.firehose_client import (
    FIREHOSE_BOTO_CONFIG,
    FirehoseDeliveryError,
    deliver_record,
)


@pytest.fixture
def mock_client():
    """Create a MagicMock boto3 Firehose client."""
    return MagicMock()


@pytest.fixture
def sample_record():
    """A sample 16-field Normalized_Record."""
    return {
        "ingestion_id": "abc-123",
        "event_id": "evt-456",
        "request_id": "req-789",
        "received_at": "2024-01-15T09:30:00.123Z",
        "schema_version": 1,
        "environment": "dev",
        "event_type": "MESSAGE_RECEIVED",
        "conversation_id": "conv-001",
        "message_id": "msg-002",
        "user_id": "usr-003",
        "order_form_id": None,
        "source_channel": "WHATSAPP",
        "source_bot": "bot-main",
        "severity": None,
        "error_code": None,
        "raw_event_json": '{"eventId":"evt-456","eventType":"MESSAGE_RECEIVED"}',
    }


STREAM_NAME = "test-delivery-stream"


class TestDeliverRecordSuccess:
    """Test successful PutRecord calls."""

    def test_returns_record_id(self, mock_client, sample_record):
        mock_client.put_record.return_value = {"RecordId": "rec-id-12345"}

        result = deliver_record(sample_record, STREAM_NAME, mock_client)

        assert result == "rec-id-12345"

    def test_calls_put_record_with_correct_stream_name(self, mock_client, sample_record):
        mock_client.put_record.return_value = {"RecordId": "rec-id-001"}

        deliver_record(sample_record, STREAM_NAME, mock_client)

        call_kwargs = mock_client.put_record.call_args[1]
        assert call_kwargs["DeliveryStreamName"] == STREAM_NAME

    def test_serializes_record_as_compact_json_utf8(self, mock_client, sample_record):
        mock_client.put_record.return_value = {"RecordId": "rec-id-001"}

        deliver_record(sample_record, STREAM_NAME, mock_client)

        call_kwargs = mock_client.put_record.call_args[1]
        data_bytes = call_kwargs["Record"]["Data"]

        # Verify it's bytes
        assert isinstance(data_bytes, bytes)

        # Verify it decodes as valid JSON matching the record
        decoded = json.loads(data_bytes.decode("utf-8"))
        assert decoded == sample_record

    def test_serializes_without_spaces(self, mock_client, sample_record):
        mock_client.put_record.return_value = {"RecordId": "rec-id-001"}

        deliver_record(sample_record, STREAM_NAME, mock_client)

        call_kwargs = mock_client.put_record.call_args[1]
        data_str = call_kwargs["Record"]["Data"].decode("utf-8")

        # Compact JSON: no spaces after separators
        expected = json.dumps(sample_record, ensure_ascii=False, separators=(",", ":"))
        assert data_str == expected

    def test_handles_unicode_in_record(self, mock_client):
        record = {
            "ingestion_id": "id-1",
            "event_type": "MSG",
            "raw_event_json": '{"msg":"Hola señor 👋"}',
        }
        mock_client.put_record.return_value = {"RecordId": "rec-unicode"}

        result = deliver_record(record, STREAM_NAME, mock_client)

        assert result == "rec-unicode"
        call_kwargs = mock_client.put_record.call_args[1]
        data_bytes = call_kwargs["Record"]["Data"]
        decoded = data_bytes.decode("utf-8")
        # ensure_ascii=False preserves unicode characters
        assert "señor" in decoded
        assert "👋" in decoded


class TestDeliverRecordFirehoseUnavailable:
    """Test Firehose service unavailable errors raise FirehoseDeliveryError."""

    def test_service_unavailable_raises_firehose_delivery_error(self, mock_client, sample_record):
        error_response = {
            "Error": {
                "Code": "ServiceUnavailableException",
                "Message": "Service is temporarily unavailable",
            }
        }
        mock_client.put_record.side_effect = ClientError(
            error_response, "PutRecord"
        )

        with pytest.raises(FirehoseDeliveryError) as exc_info:
            deliver_record(sample_record, STREAM_NAME, mock_client)

        assert STREAM_NAME in str(exc_info.value)

    def test_service_unavailable_chains_original_exception(self, mock_client, sample_record):
        error_response = {
            "Error": {
                "Code": "ServiceUnavailableException",
                "Message": "Service is temporarily unavailable",
            }
        }
        original_exc = ClientError(error_response, "PutRecord")
        mock_client.put_record.side_effect = original_exc

        with pytest.raises(FirehoseDeliveryError) as exc_info:
            deliver_record(sample_record, STREAM_NAME, mock_client)

        assert exc_info.value.__cause__ is original_exc


class TestDeliverRecordTimeout:
    """Test timeout errors raise FirehoseDeliveryError."""

    def test_connect_timeout_raises_firehose_delivery_error(self, mock_client, sample_record):
        from botocore.exceptions import ConnectTimeoutError

        mock_client.put_record.side_effect = ConnectTimeoutError(endpoint_url="https://firehose.us-east-1.amazonaws.com")

        with pytest.raises(FirehoseDeliveryError):
            deliver_record(sample_record, STREAM_NAME, mock_client)

    def test_read_timeout_raises_firehose_delivery_error(self, mock_client, sample_record):
        from botocore.exceptions import ReadTimeoutError

        mock_client.put_record.side_effect = ReadTimeoutError(endpoint_url="https://firehose.us-east-1.amazonaws.com")

        with pytest.raises(FirehoseDeliveryError):
            deliver_record(sample_record, STREAM_NAME, mock_client)

    def test_generic_botocore_error_raises_firehose_delivery_error(self, mock_client, sample_record):
        mock_client.put_record.side_effect = BotoCoreError()

        with pytest.raises(FirehoseDeliveryError):
            deliver_record(sample_record, STREAM_NAME, mock_client)


class TestDeliverRecordPermissionErrors:
    """Test permission/access errors raise FirehoseDeliveryError."""

    def test_access_denied_raises_firehose_delivery_error(self, mock_client, sample_record):
        error_response = {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "User is not authorized to perform firehose:PutRecord",
            }
        }
        mock_client.put_record.side_effect = ClientError(
            error_response, "PutRecord"
        )

        with pytest.raises(FirehoseDeliveryError) as exc_info:
            deliver_record(sample_record, STREAM_NAME, mock_client)

        assert STREAM_NAME in str(exc_info.value)

    def test_invalid_argument_raises_firehose_delivery_error(self, mock_client, sample_record):
        error_response = {
            "Error": {
                "Code": "InvalidArgumentException",
                "Message": "Invalid delivery stream name",
            }
        }
        mock_client.put_record.side_effect = ClientError(
            error_response, "PutRecord"
        )

        with pytest.raises(FirehoseDeliveryError):
            deliver_record(sample_record, STREAM_NAME, mock_client)

    def test_resource_not_found_raises_firehose_delivery_error(self, mock_client, sample_record):
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Delivery stream not found",
            }
        }
        mock_client.put_record.side_effect = ClientError(
            error_response, "PutRecord"
        )

        with pytest.raises(FirehoseDeliveryError):
            deliver_record(sample_record, STREAM_NAME, mock_client)


class TestSDKRetryConfig:
    """Verify the adaptive retry configuration constants."""

    def test_retry_mode_is_adaptive(self):
        assert FIREHOSE_BOTO_CONFIG.retries["mode"] == "adaptive"

    def test_max_attempts_is_3(self):
        assert FIREHOSE_BOTO_CONFIG.retries["max_attempts"] == 3
