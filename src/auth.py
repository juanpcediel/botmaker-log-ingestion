"""Bearer token validation with secret caching.

Handles authentication of incoming webhook requests by validating
Bearer tokens against a secret stored in AWS Secrets Manager.
Uses in-memory caching with TTL to minimize API calls.

Requirements: 1.3, 16.1, 16.5, 9.4
"""

from __future__ import annotations

import hmac
import time
from typing import Any, Optional

from config import SECRET_CACHE_TTL, SECRET_TIMEOUT
from logger import StructuredLogger, EVENT_TYPE_INGESTION_RECEIVED


class SecretCache:
    """In-memory cache with TTL for Secrets Manager values.

    Caches the webhook secret to avoid calling Secrets Manager on
    every request. Automatically re-fetches when the TTL expires.

    The default TTL is 5 minutes (300 seconds), matching the
    SECRET_CACHE_TTL constant from config.
    """

    def __init__(self, ttl_seconds: int = SECRET_CACHE_TTL) -> None:
        """Initialize the secret cache.

        Args:
            ttl_seconds: Time-to-live for cached values in seconds.
                Defaults to SECRET_CACHE_TTL (300s / 5 minutes).
        """
        self._value: Optional[str] = None
        self._expires_at: float = 0
        self._ttl = ttl_seconds

    def get(self, secret_id: str, client: Any) -> str:
        """Retrieve the secret value, using cache if still valid.

        Args:
            secret_id: The Secrets Manager secret ARN or name.
            client: A boto3 Secrets Manager client instance.

        Returns:
            The secret string value.

        Raises:
            SecretRetrievalError: If Secrets Manager call fails or times out.
        """
        if self._value is not None and time.time() < self._expires_at:
            return self._value

        try:
            response = client.get_secret_value(SecretId=secret_id)
            self._value = response["SecretString"]
            self._expires_at = time.time() + self._ttl
            return self._value
        except Exception as exc:
            # Invalidate cache on error so next attempt retries
            self._value = None
            self._expires_at = 0
            raise SecretRetrievalError(
                "Failed to retrieve authentication secret"
            ) from exc

    def invalidate(self) -> None:
        """Force cache invalidation, causing next get() to re-fetch."""
        self._value = None
        self._expires_at = 0


class SecretRetrievalError(Exception):
    """Raised when Secrets Manager secret cannot be retrieved.

    The error message intentionally does NOT expose the secret name
    or value to prevent information leakage (Requirement 16.5).
    """

    pass


def _get_source_ip(event: dict) -> str:
    """Extract source IP from API Gateway event, safely."""
    request_context = event.get("requestContext")
    if isinstance(request_context, dict):
        http_info = request_context.get("http")
        if isinstance(http_info, dict):
            return http_info.get("sourceIp", "unknown")
    return "unknown"


def _get_authorization_header(event: dict) -> Optional[str]:
    """Extract Authorization header from event, case-insensitive.

    API Gateway HTTP API v2.0 lowercases all header names,
    but we handle both cases for robustness.

    Args:
        event: API Gateway Lambda proxy event.

    Returns:
        The Authorization header value, or None if not present.
    """
    headers = event.get("headers")
    if not isinstance(headers, dict):
        return None

    # API Gateway HTTP API v2.0 normalizes headers to lowercase
    for key, value in headers.items():
        if key.lower() == "authorization":
            return value

    return None


def _build_401_response() -> dict:
    """Build a standard HTTP 401 Unauthorized response.

    The response body does NOT contain credential values or
    the expected secret (Requirement 1.3, 9.4).
    """
    return {
        "statusCode": 401,
        "headers": {"Content-Type": "application/json"},
        "body": '{"error": "Unauthorized", "message": "Invalid or missing authentication token"}',
    }


def _build_500_response() -> dict:
    """Build a standard HTTP 500 Internal Server Error response.

    Used when Secrets Manager is unreachable or times out.
    Does NOT expose the secret name or value (Requirement 16.5).
    """
    return {
        "statusCode": 500,
        "headers": {"Content-Type": "application/json"},
        "body": '{"error": "Internal Server Error"}',
    }


def validate_token(
    event: dict,
    secret_cache: SecretCache,
    sm_client: Any,
    secret_id: str,
    logger: Optional[StructuredLogger] = None,
) -> tuple[bool, Optional[dict]]:
    """Validate the Bearer token from the request Authorization header.

    Extracts the token from the Authorization header, retrieves the
    expected secret from cache/Secrets Manager, and performs a
    constant-time comparison using hmac.compare_digest.

    Args:
        event: API Gateway Lambda proxy event (v2.0 format).
        secret_cache: SecretCache instance for caching the expected token.
        sm_client: boto3 Secrets Manager client.
        secret_id: The secret ARN/name to retrieve from Secrets Manager.
        logger: Optional StructuredLogger for logging rejection events.

    Returns:
        A tuple of (is_valid, error_response):
        - (True, None) if authentication succeeds.
        - (False, error_response_dict) if authentication fails.
    """
    source_ip = _get_source_ip(event)

    # Step 1: Extract Authorization header
    auth_header = _get_authorization_header(event)

    if auth_header is None:
        if logger:
            logger.warning(
                EVENT_TYPE_INGESTION_RECEIVED,
                outcome="failure",
                error_message="Missing Authorization header",
                payload_metadata={"source_ip": source_ip, "reason": "missing_header"},
            )
        return False, _build_401_response()

    # Step 2: Verify Bearer prefix
    if not auth_header.startswith("Bearer "):
        if logger:
            logger.warning(
                EVENT_TYPE_INGESTION_RECEIVED,
                outcome="failure",
                error_message="Malformed Authorization header",
                payload_metadata={"source_ip": source_ip, "reason": "malformed_header"},
            )
        return False, _build_401_response()

    # Step 3: Extract token value
    provided_token = auth_header[7:]  # len("Bearer ") == 7

    if not provided_token:
        if logger:
            logger.warning(
                EVENT_TYPE_INGESTION_RECEIVED,
                outcome="failure",
                error_message="Empty Bearer token",
                payload_metadata={"source_ip": source_ip, "reason": "empty_token"},
            )
        return False, _build_401_response()

    # Step 4: Get expected secret from cache/Secrets Manager
    try:
        expected_secret = secret_cache.get(secret_id, sm_client)
    except SecretRetrievalError:
        # Log failure without exposing secret name or value (Req 16.5)
        if logger:
            logger.error(
                EVENT_TYPE_INGESTION_RECEIVED,
                outcome="failure",
                error_message="Authentication secret retrieval failed",
                payload_metadata={"source_ip": source_ip, "reason": "secret_retrieval_error"},
            )
        return False, _build_500_response()

    # Step 5: Constant-time comparison (Req 1.3 - secure comparison)
    is_valid = hmac.compare_digest(
        provided_token.encode("utf-8"),
        expected_secret.encode("utf-8"),
    )

    if not is_valid:
        if logger:
            logger.warning(
                EVENT_TYPE_INGESTION_RECEIVED,
                outcome="failure",
                error_message="Invalid authentication token",
                payload_metadata={"source_ip": source_ip, "reason": "invalid_token"},
            )
        return False, _build_401_response()

    return True, None
