"""Unit tests for src/auth.py — Bearer token validation with secret caching.

Validates:
- Valid token authentication succeeds
- Invalid token returns 401
- Missing Authorization header returns 401
- Malformed header (no "Bearer " prefix) returns 401
- Secrets Manager errors (timeout, access denied)
- Secret caching (cache hit, cache expiry, re-fetch)
- Response bodies do not contain credential values

Requirements: 1.3, 16.1, 16.5, 9.4
"""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.auth import (
    SecretCache,
    SecretRetrievalError,
    validate_token,
    _build_401_response,
    _build_500_response,
    _get_authorization_header,
    _get_source_ip,
)
from src.logger import StructuredLogger


# --- Test Constants ---

SECRET_ID = "arn:aws:secretsmanager:us-east-1:123456789012:secret:webhook-token"
VALID_TOKEN = "my-super-secret-webhook-token-value"


# --- Fixtures ---


@pytest.fixture
def sm_client():
    """Mock boto3 Secrets Manager client."""
    client = MagicMock()
    client.get_secret_value.return_value = {"SecretString": VALID_TOKEN}
    return client


@pytest.fixture
def logger():
    """Mock StructuredLogger instance."""
    mock_logger = MagicMock(spec=StructuredLogger)
    return mock_logger


@pytest.fixture
def secret_cache():
    """Fresh SecretCache instance for each test."""
    return SecretCache(ttl_seconds=300)


@pytest.fixture
def valid_event():
    """Sample API Gateway HTTP API v2.0 event with a valid Authorization header."""
    return {
        "headers": {
            "authorization": f"Bearer {VALID_TOKEN}",
            "content-type": "application/json",
        },
        "requestContext": {
            "http": {
                "method": "POST",
                "sourceIp": "192.168.1.100",
            }
        },
        "body": '{"event_type": "MESSAGE_RECEIVED"}',
    }


# --- Tests: Valid Token Authentication ---


class TestValidTokenAuthentication:
    """Test that valid token authentication succeeds."""

    def test_valid_token_returns_true_none(self, valid_event, secret_cache, sm_client, logger):
        is_valid, response = validate_token(valid_event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is True
        assert response is None

    def test_valid_token_calls_secrets_manager(self, valid_event, secret_cache, sm_client, logger):
        validate_token(valid_event, secret_cache, sm_client, SECRET_ID, logger)

        sm_client.get_secret_value.assert_called_once_with(SecretId=SECRET_ID)

    def test_valid_token_without_logger(self, valid_event, secret_cache, sm_client):
        is_valid, response = validate_token(valid_event, secret_cache, sm_client, SECRET_ID, None)

        assert is_valid is True
        assert response is None


# --- Tests: Invalid Token Returns 401 ---


class TestInvalidToken:
    """Test that invalid token returns 401."""

    def test_wrong_token_returns_false_with_401(self, secret_cache, sm_client, logger):
        event = {
            "headers": {"authorization": "Bearer wrong-token-value"},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        is_valid, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 401

    def test_empty_bearer_token_returns_401(self, secret_cache, sm_client, logger):
        event = {
            "headers": {"authorization": "Bearer "},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        is_valid, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 401

    def test_invalid_token_logs_warning(self, secret_cache, sm_client, logger):
        event = {
            "headers": {"authorization": "Bearer invalid-token"},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        logger.warning.assert_called_once()


# --- Tests: Missing Authorization Header Returns 401 ---


class TestMissingAuthorizationHeader:
    """Test that missing Authorization header returns 401."""

    def test_no_authorization_header_returns_401(self, secret_cache, sm_client, logger):
        event = {
            "headers": {"content-type": "application/json"},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        is_valid, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 401

    def test_no_headers_dict_returns_401(self, secret_cache, sm_client, logger):
        event = {
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        is_valid, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 401

    def test_headers_not_dict_returns_401(self, secret_cache, sm_client, logger):
        event = {
            "headers": None,
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        is_valid, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 401

    def test_missing_header_logs_warning(self, secret_cache, sm_client, logger):
        event = {
            "headers": {},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        logger.warning.assert_called_once()


# --- Tests: Malformed Header (no "Bearer " prefix) Returns 401 ---


class TestMalformedHeader:
    """Test that malformed header (no Bearer prefix) returns 401."""

    def test_no_bearer_prefix_returns_401(self, secret_cache, sm_client, logger):
        event = {
            "headers": {"authorization": f"Basic {VALID_TOKEN}"},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        is_valid, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 401

    def test_token_without_scheme_returns_401(self, secret_cache, sm_client, logger):
        event = {
            "headers": {"authorization": VALID_TOKEN},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        is_valid, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 401

    def test_lowercase_bearer_returns_401(self, secret_cache, sm_client, logger):
        """Bearer prefix is case-sensitive per HTTP spec."""
        event = {
            "headers": {"authorization": f"bearer {VALID_TOKEN}"},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        is_valid, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 401

    def test_malformed_header_logs_warning(self, secret_cache, sm_client, logger):
        event = {
            "headers": {"authorization": "Token abc123"},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        logger.warning.assert_called_once()


# --- Tests: Secrets Manager Errors ---


class TestSecretsManagerErrors:
    """Test Secrets Manager errors (timeout, access denied)."""

    def test_timeout_error_returns_500(self, valid_event, secret_cache, logger):
        sm_client = MagicMock()
        sm_client.get_secret_value.side_effect = Exception("Read timed out")

        is_valid, response = validate_token(valid_event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 500

    def test_access_denied_returns_500(self, valid_event, secret_cache, logger):
        sm_client = MagicMock()
        sm_client.get_secret_value.side_effect = Exception(
            "AccessDeniedException: User is not authorized"
        )

        is_valid, response = validate_token(valid_event, secret_cache, sm_client, SECRET_ID, logger)

        assert is_valid is False
        assert response["statusCode"] == 500

    def test_secrets_manager_error_logs_error(self, valid_event, secret_cache, logger):
        sm_client = MagicMock()
        sm_client.get_secret_value.side_effect = Exception("Connection refused")

        validate_token(valid_event, secret_cache, sm_client, SECRET_ID, logger)

        logger.error.assert_called_once()

    def test_secret_retrieval_error_raised_from_cache(self):
        sm_client = MagicMock()
        sm_client.get_secret_value.side_effect = Exception("Network error")

        cache = SecretCache()

        with pytest.raises(SecretRetrievalError, match="Failed to retrieve authentication secret"):
            cache.get(SECRET_ID, sm_client)

    def test_secret_retrieval_error_preserves_cause(self):
        original = ValueError("Original cause")
        sm_client = MagicMock()
        sm_client.get_secret_value.side_effect = original

        cache = SecretCache()

        with pytest.raises(SecretRetrievalError) as exc_info:
            cache.get(SECRET_ID, sm_client)

        assert exc_info.value.__cause__ is original


# --- Tests: Secret Caching ---


class TestSecretCaching:
    """Test secret caching (cache hit, cache expiry, re-fetch)."""

    def test_cache_hit_does_not_call_client(self, sm_client):
        cache = SecretCache(ttl_seconds=300)

        # First call fetches from client
        result1 = cache.get(SECRET_ID, sm_client)
        # Second call should use cache
        result2 = cache.get(SECRET_ID, sm_client)

        assert result1 == VALID_TOKEN
        assert result2 == VALID_TOKEN
        sm_client.get_secret_value.assert_called_once()

    @patch("src.auth.time.time")
    def test_cache_expiry_triggers_refetch(self, mock_time, sm_client):
        cache = SecretCache(ttl_seconds=300)

        # First call at t=1000
        mock_time.return_value = 1000.0
        cache.get(SECRET_ID, sm_client)
        assert sm_client.get_secret_value.call_count == 1

        # Second call at t=1100 (within TTL)
        mock_time.return_value = 1100.0
        cache.get(SECRET_ID, sm_client)
        assert sm_client.get_secret_value.call_count == 1  # Still cached

        # Third call at t=1301 (TTL expired: 1000 + 300 = 1300)
        mock_time.return_value = 1301.0
        cache.get(SECRET_ID, sm_client)
        assert sm_client.get_secret_value.call_count == 2  # Re-fetched

    def test_cache_invalidation_forces_refetch(self, sm_client):
        cache = SecretCache(ttl_seconds=300)

        cache.get(SECRET_ID, sm_client)
        assert sm_client.get_secret_value.call_count == 1

        cache.invalidate()
        cache.get(SECRET_ID, sm_client)
        assert sm_client.get_secret_value.call_count == 2

    def test_cache_cleared_on_error(self, sm_client):
        cache = SecretCache(ttl_seconds=300)

        # First call succeeds and caches
        cache.get(SECRET_ID, sm_client)
        assert sm_client.get_secret_value.call_count == 1

        # Invalidate and set up client to fail
        cache.invalidate()
        sm_client.get_secret_value.side_effect = Exception("Temporary failure")

        with pytest.raises(SecretRetrievalError):
            cache.get(SECRET_ID, sm_client)

        # After error, cache should be empty; next call should retry
        sm_client.get_secret_value.side_effect = None
        sm_client.get_secret_value.return_value = {"SecretString": "new-secret"}

        result = cache.get(SECRET_ID, sm_client)
        assert result == "new-secret"

    @patch("src.auth.time.time")
    def test_custom_ttl_respected(self, mock_time):
        sm_client = MagicMock()
        sm_client.get_secret_value.return_value = {"SecretString": "short-lived"}

        cache = SecretCache(ttl_seconds=10)

        mock_time.return_value = 1000.0
        cache.get(SECRET_ID, sm_client)
        assert sm_client.get_secret_value.call_count == 1

        # At t=1009 (within 10s TTL)
        mock_time.return_value = 1009.0
        cache.get(SECRET_ID, sm_client)
        assert sm_client.get_secret_value.call_count == 1

        # At t=1011 (past 10s TTL)
        mock_time.return_value = 1011.0
        cache.get(SECRET_ID, sm_client)
        assert sm_client.get_secret_value.call_count == 2


# --- Tests: Response Bodies Do Not Contain Credential Values ---


class TestResponseBodiesNoCredentials:
    """Test that response bodies do not contain credential values."""

    def test_401_response_body_has_no_token(self):
        response = _build_401_response()
        body = response["body"]

        assert VALID_TOKEN not in body
        assert "secret" not in body.lower() or "secret" in "authentication token"
        assert "Bearer" not in body

    def test_401_response_body_has_no_secret_id(self):
        response = _build_401_response()
        body = response["body"]

        assert SECRET_ID not in body
        assert "arn:aws" not in body

    def test_500_response_body_has_no_credentials(self):
        response = _build_500_response()
        body = response["body"]

        assert VALID_TOKEN not in body
        assert SECRET_ID not in body
        assert "arn:aws" not in body

    def test_invalid_token_response_does_not_leak_expected_secret(
        self, secret_cache, sm_client, logger
    ):
        event = {
            "headers": {"authorization": "Bearer wrong-token"},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        _, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        body = response["body"]
        assert VALID_TOKEN not in body
        assert SECRET_ID not in body

    def test_missing_header_response_does_not_leak_secret(
        self, secret_cache, sm_client, logger
    ):
        event = {
            "headers": {},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        _, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        body = response["body"]
        assert VALID_TOKEN not in body
        assert SECRET_ID not in body

    def test_secrets_manager_error_response_does_not_leak_secret(
        self, secret_cache, logger
    ):
        sm_client = MagicMock()
        sm_client.get_secret_value.side_effect = Exception("Timeout")

        event = {
            "headers": {"authorization": f"Bearer {VALID_TOKEN}"},
            "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
        }

        _, response = validate_token(event, secret_cache, sm_client, SECRET_ID, logger)

        body = response["body"]
        assert VALID_TOKEN not in body
        assert SECRET_ID not in body
        assert "Timeout" not in body


# --- Tests: Helper Functions ---


class TestHelperFunctions:
    """Tests for internal helper functions."""

    def test_get_source_ip_from_valid_event(self):
        event = {"requestContext": {"http": {"sourceIp": "203.0.113.50"}}}
        assert _get_source_ip(event) == "203.0.113.50"

    def test_get_source_ip_missing_request_context(self):
        assert _get_source_ip({}) == "unknown"

    def test_get_source_ip_missing_http(self):
        event = {"requestContext": {}}
        assert _get_source_ip(event) == "unknown"

    def test_get_source_ip_missing_source_ip_field(self):
        event = {"requestContext": {"http": {"method": "POST"}}}
        assert _get_source_ip(event) == "unknown"

    def test_get_authorization_header_lowercase(self):
        event = {"headers": {"authorization": "Bearer token123"}}
        assert _get_authorization_header(event) == "Bearer token123"

    def test_get_authorization_header_mixed_case(self):
        event = {"headers": {"Authorization": "Bearer token123"}}
        assert _get_authorization_header(event) == "Bearer token123"

    def test_get_authorization_header_missing(self):
        event = {"headers": {"content-type": "application/json"}}
        assert _get_authorization_header(event) is None

    def test_get_authorization_header_no_headers(self):
        event = {}
        assert _get_authorization_header(event) is None
