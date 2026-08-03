"""
Production smoke test suite for the Botmaker Log Ingestion Pipeline.

These tests run against a DEPLOYED prod stack after deployment.
They verify basic pipeline health without polluting analytics data
by using event_type=PIPELINE_SMOKE_TEST.

Environment variables (set by buildspec-smoke.yml):
    API_ENDPOINT — API Gateway URL
    AUTH_TOKEN   — Bearer token for webhook authentication
    STACK_NAME   — CloudFormation stack name

Validates: Requirements 14.1, 14.2, 14.3, 14.4, 14.5
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
import pytest
import requests


pytestmark = pytest.mark.smoke


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"Environment variable {name} not set — skipping smoke tests")
    return value


@pytest.fixture(scope="session")
def api_endpoint() -> str:
    return _require_env("API_ENDPOINT").rstrip("/")


@pytest.fixture(scope="session")
def auth_token() -> str:
    return _require_env("AUTH_TOKEN")


@pytest.fixture(scope="session")
def stack_name() -> str:
    return _require_env("STACK_NAME")


@pytest.fixture(scope="session")
def smoke_event_id() -> str:
    """Generate a unique event_id per smoke test run."""
    return f"smoke-{uuid.uuid4()}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_smoke_event(event_id: str) -> dict:
    """Build a smoke test payload with event_type=PIPELINE_SMOKE_TEST."""
    return {
        "eventId": event_id,
        "eventType": "PIPELINE_SMOKE_TEST",
        "conversation": {
            "conversationId": f"smoke-conv-{event_id}",
            "messageId": f"smoke-msg-{event_id}",
            "userId": "smoke-test-user",
        },
        "source": {
            "channel": "smoke-test",
            "bot": "smoke-bot",
        },
        "smoke_test": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _poll_cloudwatch_for_firehose_accepted(
    stack_name: str,
    event_id: str,
    start_time_ms: int,
    timeout_seconds: int = 30,
) -> bool:
    """
    Poll CloudWatch Logs for FIREHOSE_ACCEPTED entry matching the event_id.

    Returns True if found within timeout, False otherwise.
    """
    logs_client = boto3.client("logs")

    # Find Lambda log groups for the stack
    log_groups = []
    paginator = logs_client.get_paginator("describe_log_groups")
    for page in paginator.paginate(logGroupNamePrefix=f"/aws/lambda/{stack_name}"):
        for lg in page.get("logGroups", []):
            log_groups.append(lg["logGroupName"])

    if not log_groups:
        return False

    deadline = time.time() + timeout_seconds
    poll_interval = 3

    while time.time() < deadline:
        for log_group in log_groups:
            try:
                response = logs_client.filter_log_events(
                    logGroupName=log_group,
                    filterPattern=f'"{event_id}" "FIREHOSE_ACCEPTED"',
                    startTime=start_time_ms,
                    limit=10,
                )
                events = response.get("events", [])
                if events:
                    return True
            except logs_client.exceptions.ResourceNotFoundException:
                continue
        time.sleep(poll_interval)

    return False


# ---------------------------------------------------------------------------
# Smoke Tests
# ---------------------------------------------------------------------------


class SmokeTestResults:
    """Tracks individual smoke test step results for final reporting."""

    def __init__(self):
        self.steps: dict[str, bool] = {}
        self.failures: list[str] = []

    def record(self, step_name: str, passed: bool, reason: str = ""):
        self.steps[step_name] = passed
        if not passed:
            self.failures.append(f"{step_name}: {reason}" if reason else step_name)

    @property
    def all_passed(self) -> bool:
        return all(self.steps.values())


@pytest.fixture(scope="session")
def smoke_results() -> SmokeTestResults:
    return SmokeTestResults()


def test_smoke_event_accepted(
    api_endpoint, auth_token, smoke_event_id, smoke_results
):
    """
    Submit event with event_type=PIPELINE_SMOKE_TEST and verify HTTP 202.

    Validates: Requirement 14.1
    """
    payload = _build_smoke_event(smoke_event_id)
    response = requests.post(
        f"{api_endpoint}/webhook",
        json=payload,
        headers={"Authorization": f"Bearer {auth_token}"},
        timeout=10,
    )

    passed = response.status_code == 202
    smoke_results.record(
        "event_accepted",
        passed,
        reason=f"Got HTTP {response.status_code}" if not passed else "",
    )

    assert passed, (
        f"Smoke event not accepted: expected HTTP 202, got {response.status_code}. "
        f"Response: {response.text}"
    )

    # Verify response body contains ingestion_id
    body = response.json()
    assert "ingestion_id" in body, "Response missing ingestion_id field"


def test_smoke_event_logged_in_cloudwatch(
    api_endpoint, auth_token, smoke_event_id, stack_name, smoke_results
):
    """
    Poll CloudWatch (30s) for FIREHOSE_ACCEPTED for the smoke test event_id.

    Validates: Requirements 14.2, 14.3, 14.4
    """
    # Ensure the event was sent first
    start_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - 5000

    payload = _build_smoke_event(smoke_event_id)
    response = requests.post(
        f"{api_endpoint}/webhook",
        json=payload,
        headers={"Authorization": f"Bearer {auth_token}"},
        timeout=10,
    )

    if response.status_code != 202:
        smoke_results.record(
            "cloudwatch_verification",
            False,
            reason=f"Event submission failed with HTTP {response.status_code}",
        )
        pytest.fail(f"Cannot verify logs — event submission failed: HTTP {response.status_code}")

    # Poll CloudWatch for FIREHOSE_ACCEPTED within 30 seconds
    found = _poll_cloudwatch_for_firehose_accepted(
        stack_name=stack_name,
        event_id=smoke_event_id,
        start_time_ms=start_time_ms,
        timeout_seconds=30,
    )

    smoke_results.record(
        "cloudwatch_verification",
        found,
        reason="FIREHOSE_ACCEPTED not found within 30s" if not found else "",
    )

    assert found, (
        f"FIREHOSE_ACCEPTED log entry not found for event_id={smoke_event_id} "
        f"within 30 seconds — pipeline may be unhealthy"
    )


def test_smoke_final_report(smoke_results):
    """
    Verify all smoke test steps pass, report which failed if any.

    Validates: Requirement 14.5
    """
    if not smoke_results.steps:
        pytest.skip("No smoke test steps were recorded")

    if smoke_results.all_passed:
        # All verifications passed
        return

    # Report which steps failed
    failure_report = "; ".join(smoke_results.failures)
    pytest.fail(
        f"Smoke test FAILED — {len(smoke_results.failures)} step(s) did not succeed: "
        f"{failure_report}"
    )
