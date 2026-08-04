"""Unit tests for src/handler.py — Lambda entry point.

Tests cover:
- Full happy path: valid auth, valid JSON → 202 with ingestion_id
- Invalid JSON body → 400
- Array body → 400
- Primitive bodies (string, number, boolean, null) → 400
- Base64-encoded body decoding
- Oversized payload (>6 MB) → 413
- Oversized decoded body (>1 MB) → 400
- Empty body → 400
- Firehose failure → 503
- Correlation ID propagation

Requirements: 1.2, 1.4, 1.5, 4.1, 4.3, 4.4, 5.12, 5.13, 9.2, 9.5
"""

from __future__ import annotations

import base64
import json
import os
from unittest.mock import MagicMock, patch

import pytest

# Set environment variables before importing handler
os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("FIREHOSE_STREAM_NAME", "test-stream")
os.environ.setdefault("SECRET_ID", "test-secret")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_config_cache():
    """Clear the config LRU cache between tests."""
    from config import get_config
    get_config.cache_clear()
    yield
    get_config.cache_clear()


@pytest.fixture()
def mock_sm_client():
    """Patch the module-level Secrets Manager client."""
    with patch("src.handler._sm_client") as mock:
        mock.get_secret_value.return_value = {"SecretString": "valid-token-123"}
        yield mock


@pytest.fixture()
def mock_firehose_client():
    """Patch the module-level Firehose client."""
    with patch("src.handler._firehose_client") as mock:
        mock.put_record.return_value = {"RecordId": "mock-record-id-001"}
        yield mock


@pytest.fixture()
def mock_secret_cache():
    """Patch the module-level SecretCache."""
    with patch("src.handler._secret_cache") as mock:
        mock.get.return_value = "valid-token-123"
        yield mock


@pytest.fixture()
def valid_event():
    """Build a minimal valid API Gateway v2.0 event."""
    return {
        "requestContext": {
            "requestId": "apigw-req-id-abc",
            "http": {"sourceIp": "1.2.3.4"},
        },
        "headers": {"authorization": "Bearer valid-token-123"},
        "body": json.dumps({"eventId": "e1", "eventType": "MESSAGE_RECEIVED"}),
        "isBase64Encoded": False,
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    """Valid auth + valid JSON → 202 with ingestion_id."""

    def test_returns_202_with_ingestion_id(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 202
        body = json.loads(response["body"])
        assert body["status"] == "accepted"
        assert "ingestion_id" in body
        # ingestion_id should be a UUID-like string
        assert len(body["ingestion_id"]) == 36

    def test_firehose_put_record_called(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        lambda_handler(valid_event, None)

        mock_firehose_client.put_record.assert_called_once()
        call_kwargs = mock_firehose_client.put_record.call_args[1]
        assert call_kwargs["DeliveryStreamName"] == "test-stream"
        assert "Data" in call_kwargs["Record"]


# ---------------------------------------------------------------------------
# Invalid JSON body → 400
# ---------------------------------------------------------------------------


class TestInvalidJsonBody:
    """Non-parseable JSON bodies should return 400."""

    def test_invalid_json_returns_400(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        valid_event["body"] = "not-json{{"
        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "not parseable JSON" in body["message"]

    def test_empty_body_returns_400(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        valid_event["body"] = ""
        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 400


# ---------------------------------------------------------------------------
# Array body → 400
# ---------------------------------------------------------------------------


class TestArrayBody:
    """JSON arrays are not valid webhook bodies."""

    def test_array_body_returns_400(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        valid_event["body"] = json.dumps([1, 2, 3])
        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "not a valid JSON object" in body["message"]


# ---------------------------------------------------------------------------
# Primitive bodies → 400
# ---------------------------------------------------------------------------


class TestPrimitiveBodies:
    """String, number, boolean, null JSON primitives should return 400."""

    @pytest.mark.parametrize(
        "primitive",
        [
            json.dumps("hello"),
            json.dumps(42),
            json.dumps(3.14),
            json.dumps(True),
            json.dumps(False),
            json.dumps(None),
        ],
        ids=["string", "integer", "float", "true", "false", "null"],
    )
    def test_primitive_json_returns_400(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache, primitive
    ):
        from handler import lambda_handler

        valid_event["body"] = primitive
        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 400


# ---------------------------------------------------------------------------
# Base64-encoded body decoding
# ---------------------------------------------------------------------------


class TestBase64Decoding:
    """Base64-encoded bodies should be decoded before processing."""

    def test_base64_body_decoded_returns_202(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        payload = json.dumps({"eventId": "b64-evt", "eventType": "TEST"})
        valid_event["body"] = base64.b64encode(payload.encode("utf-8")).decode("ascii")
        valid_event["isBase64Encoded"] = True

        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 202
        body = json.loads(response["body"])
        assert body["status"] == "accepted"

    def test_invalid_base64_returns_400(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        # Valid base64 that doesn't decode to valid UTF-8 JSON
        valid_event["body"] = base64.b64encode(b"\xff\xfe invalid").decode("ascii")
        valid_event["isBase64Encoded"] = True

        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 400


# ---------------------------------------------------------------------------
# Oversized payload (>6 MB) → 413
# ---------------------------------------------------------------------------


class TestOversizedPayload:
    """Raw payload exceeding 6 MB should return 413."""

    def test_oversized_raw_payload_returns_413(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        # Create a payload just over 6 MB
        valid_event["body"] = "x" * (6 * 1024 * 1024 + 1)
        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 413
        body = json.loads(response["body"])
        assert "exceeds maximum allowed size" in body["message"]


# ---------------------------------------------------------------------------
# Oversized decoded body (>1 MB) → 400
# ---------------------------------------------------------------------------


class TestOversizedDecodedBody:
    """Decoded body exceeding 1 MB should return 400."""

    def test_oversized_decoded_body_returns_400(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        # Create a valid JSON body just over 1 MB but under 6 MB raw
        large_value = "a" * (1024 * 1024)
        valid_event["body"] = json.dumps({"data": large_value})
        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "exceeds maximum allowed size" in body["message"]


# ---------------------------------------------------------------------------
# Firehose failure → 503
# ---------------------------------------------------------------------------


class TestFirehoseFailure:
    """Firehose delivery failure should return 503."""

    def test_firehose_failure_returns_503(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler
        from firehose_client import FirehoseDeliveryError

        with patch("src.handler.deliver_record", side_effect=FirehoseDeliveryError("fail")):
            response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 503
        body = json.loads(response["body"])
        assert "Unable to deliver record to storage" in body["message"]


# ---------------------------------------------------------------------------
# Correlation ID propagation
# ---------------------------------------------------------------------------


class TestCorrelationId:
    """Correlation ID from requestContext.requestId propagates through the pipeline."""

    def test_correlation_id_used_as_request_id_in_record(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        valid_event["requestContext"]["requestId"] = "corr-id-xyz-789"
        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 202

        # Verify the record sent to Firehose contains the correlation_id as request_id
        call_kwargs = mock_firehose_client.put_record.call_args[1]
        record_data = json.loads(call_kwargs["Record"]["Data"].decode("utf-8"))
        assert record_data["request_id"] == "corr-id-xyz-789"

    def test_missing_request_context_generates_uuid(
        self, valid_event, mock_sm_client, mock_firehose_client, mock_secret_cache
    ):
        from handler import lambda_handler

        valid_event["requestContext"] = {}
        response = lambda_handler(valid_event, None)

        assert response["statusCode"] == 202
        # Should still work with generated UUID as correlation_id
        call_kwargs = mock_firehose_client.put_record.call_args[1]
        record_data = json.loads(call_kwargs["Record"]["Data"].decode("utf-8"))
        assert record_data["request_id"] is not None
        assert len(record_data["request_id"]) == 36  # UUID format
