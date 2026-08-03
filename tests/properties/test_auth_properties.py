"""Property-based tests for authentication rejection.

Property 7: Invalid Authentication Rejection
For any string that does not exactly match the configured webhook secret,
the Ingestion_Lambda SHALL return HTTP 401 and the response body SHALL NOT
contain the expected secret value.

Minimum 100 Hypothesis examples.
Validates: Requirements 1.3
"""

import os

os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("FIREHOSE_STREAM_NAME", "test")
os.environ.setdefault("SECRET_ID", "test")

from unittest.mock import MagicMock

from hypothesis import given, settings, assume
from hypothesis import strategies as st

from src.auth import SecretCache, validate_token

CONFIGURED_SECRET = "the-real-webhook-secret-value-12345"


@settings(max_examples=100, deadline=5000)
@given(token=st.text(min_size=0, max_size=200))
def test_invalid_token_always_returns_401(token):
    """Any token != configured secret -> 401, response never contains the secret.

    **Validates: Requirements 1.3**
    """
    assume(token != CONFIGURED_SECRET)

    sm_client = MagicMock()
    sm_client.get_secret_value.return_value = {"SecretString": CONFIGURED_SECRET}
    cache = SecretCache(ttl_seconds=300)

    event = {
        "headers": {"authorization": f"Bearer {token}"},
        "requestContext": {"http": {"sourceIp": "10.0.0.1"}},
    }

    is_valid, response = validate_token(event, cache, sm_client, "secret-id", None)

    assert is_valid is False
    assert response["statusCode"] == 401
    assert CONFIGURED_SECRET not in response["body"]
