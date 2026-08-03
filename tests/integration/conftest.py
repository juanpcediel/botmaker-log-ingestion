"""
Fixtures for integration tests against a deployed dev stack.

Environment variables (set by buildspec-integration.yml):
    API_ENDPOINT   — API Gateway URL (e.g. https://xxx.execute-api.us-east-1.amazonaws.com)
    AUTH_TOKEN     — Bearer token for webhook authentication
    LOGS_BUCKET    — S3 bucket name for Parquet delivery
    GLUE_DATABASE  — Glue Data Catalog database name
    ATHENA_WORKGROUP — Athena workgroup name
    STACK_NAME     — CloudFormation stack name
"""

import os
import uuid

import pytest


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"Environment variable {name} not set — skipping integration tests")
    return value


@pytest.fixture(scope="session")
def api_endpoint() -> str:
    return _require_env("API_ENDPOINT").rstrip("/")


@pytest.fixture(scope="session")
def auth_token() -> str:
    return _require_env("AUTH_TOKEN")


@pytest.fixture(scope="session")
def logs_bucket() -> str:
    return _require_env("LOGS_BUCKET")


@pytest.fixture(scope="session")
def glue_database() -> str:
    return _require_env("GLUE_DATABASE")


@pytest.fixture(scope="session")
def athena_workgroup() -> str:
    return _require_env("ATHENA_WORKGROUP")


@pytest.fixture(scope="session")
def stack_name() -> str:
    return _require_env("STACK_NAME")


@pytest.fixture(scope="session")
def unique_event_id() -> str:
    """Generate a unique event_id per test session for isolation."""
    return f"integ-test-{uuid.uuid4()}"
