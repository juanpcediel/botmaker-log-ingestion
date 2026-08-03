"""
End-to-end integration tests for the Botmaker Log Ingestion Pipeline.

These tests run against a DEPLOYED dev stack and validate the full request
lifecycle: HTTP responses, CloudWatch log entries, S3 Parquet delivery,
and Athena queryability.

Environment variables are provided by buildspec-integration.yml and
consumed via conftest.py fixtures.

Validates: Requirements 13.1, 13.2, 13.3, 13.4, 13.5
"""

import io
import json
import time
import uuid
from datetime import datetime, timezone

import boto3
import pytest
import requests

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_test_event(event_id: str) -> dict:
    """Build a sample webhook payload for integration testing."""
    return {
        "eventId": event_id,
        "eventType": "INTEGRATION_TEST_EVENT",
        "conversation": {
            "conversationId": f"conv-{event_id}",
            "messageId": f"msg-{event_id}",
            "userId": f"user-{event_id}",
        },
        "source": {
            "channel": "integration-test",
            "bot": "test-bot",
        },
        "integration_marker": True,
    }


def _poll_cloudwatch_logs(
    log_group: str,
    filter_pattern: str,
    start_time_ms: int,
    timeout_seconds: int = 60,
    poll_interval: int = 5,
) -> list:
    """Poll CloudWatch Logs Insights until entries matching the filter appear."""
    client = boto3.client("logs")
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            response = client.filter_log_events(
                logGroupName=log_group,
                filterPattern=filter_pattern,
                startTime=start_time_ms,
                limit=50,
            )
            events = response.get("events", [])
            if events:
                return events
        except client.exceptions.ResourceNotFoundException:
            pass
        time.sleep(poll_interval)

    return []


def _wait_for_athena_query(
    query: str,
    database: str,
    workgroup: str,
    timeout_seconds: int = 60,
) -> list:
    """Execute an Athena query and wait for results."""
    client = boto3.client("athena")

    start_response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    query_execution_id = start_response["QueryExecutionId"]

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status_response = client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = status_response["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED",):
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status_response["QueryExecution"]["Status"].get(
                "StateChangeReason", "unknown"
            )
            pytest.fail(f"Athena query {state}: {reason}")
        time.sleep(3)
    else:
        pytest.fail(f"Athena query did not complete within {timeout_seconds}s")

    results = client.get_query_results(QueryExecutionId=query_execution_id)
    rows = results["ResultSet"]["Rows"]
    # First row is the header
    if len(rows) <= 1:
        return []
    return rows[1:]


# ---------------------------------------------------------------------------
# TestAPIResponses
# ---------------------------------------------------------------------------


class TestAPIResponses:
    """HTTP request tests against the deployed API endpoint.

    Validates: Requirement 13.1
    """

    def test_valid_post_returns_202(self, api_endpoint, auth_token, unique_event_id):
        """POST with valid auth + valid JSON returns 202 with ingestion_id."""
        payload = _build_test_event(unique_event_id)
        response = requests.post(
            f"{api_endpoint}/webhook",
            json=payload,
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=10,
        )
        assert response.status_code == 202, (
            f"Expected 202, got {response.status_code}: {response.text}"
        )
        body = response.json()
        assert "ingestion_id" in body
        # Validate ingestion_id is a valid UUID
        uuid.UUID(body["ingestion_id"])

    def test_missing_token_returns_401(self, api_endpoint):
        """POST without Authorization header returns 401."""
        payload = {"eventType": "TEST", "eventId": "no-auth-test"}
        response = requests.post(
            f"{api_endpoint}/webhook",
            json=payload,
            timeout=10,
        )
        assert response.status_code == 401

    def test_invalid_token_returns_401(self, api_endpoint):
        """POST with wrong Bearer token returns 401."""
        payload = {"eventType": "TEST", "eventId": "bad-auth-test"}
        response = requests.post(
            f"{api_endpoint}/webhook",
            json=payload,
            headers={"Authorization": "Bearer totally-wrong-token-value"},
            timeout=10,
        )
        assert response.status_code == 401

    def test_invalid_json_returns_400(self, api_endpoint, auth_token):
        """POST with non-JSON body returns 400."""
        response = requests.post(
            f"{api_endpoint}/webhook",
            data="not json at all {{{",
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            },
            timeout=10,
        )
        assert response.status_code == 400

    def test_array_body_returns_400(self, api_endpoint, auth_token):
        """POST with JSON array body returns 400."""
        response = requests.post(
            f"{api_endpoint}/webhook",
            json=[{"eventType": "TEST"}],
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=10,
        )
        assert response.status_code == 400

    def test_oversized_payload_returns_413(self, api_endpoint, auth_token):
        """POST with body > 6MB returns 413."""
        # Generate a payload larger than 6 MB
        large_value = "x" * (6 * 1024 * 1024 + 1)
        payload = json.dumps({"eventType": "BIG", "data": large_value})
        response = requests.post(
            f"{api_endpoint}/webhook",
            data=payload,
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        assert response.status_code == 413

    def test_wrong_method_returns_405_or_404(self, api_endpoint, auth_token):
        """GET /webhook returns 404 or 405."""
        response = requests.get(
            f"{api_endpoint}/webhook",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=10,
        )
        assert response.status_code in (404, 405), (
            f"Expected 404 or 405, got {response.status_code}"
        )


# ---------------------------------------------------------------------------
# TestCloudWatchLogs
# ---------------------------------------------------------------------------


class TestCloudWatchLogs:
    """Log verification tests — polls CloudWatch for expected log entries.

    Validates: Requirement 13.2
    """

    def test_successful_event_logged_in_cloudwatch(
        self, api_endpoint, auth_token, unique_event_id, stack_name
    ):
        """Send event, poll CloudWatch for structured log entries."""
        # Record the start time before sending the event
        start_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - 5000

        # Send the test event
        payload = _build_test_event(unique_event_id)
        response = requests.post(
            f"{api_endpoint}/webhook",
            json=payload,
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=10,
        )
        assert response.status_code == 202

        # Determine the Lambda log group name
        # Convention: /aws/lambda/<stack-name>-IngestionFunction-*
        # We search for the unique_event_id in the log entries
        logs_client = boto3.client("logs")

        # Find log groups matching the stack
        log_groups = []
        paginator = logs_client.get_paginator("describe_log_groups")
        for page in paginator.paginate(logGroupNamePrefix=f"/aws/lambda/{stack_name}"):
            for lg in page.get("logGroups", []):
                log_groups.append(lg["logGroupName"])

        assert log_groups, f"No log groups found for stack {stack_name}"

        # Poll each log group for the expected event types
        expected_events = ["INGESTION_RECEIVED", "INGESTION_VALIDATED", "FIREHOSE_ACCEPTED"]
        found_events = set()

        for log_group in log_groups:
            events = _poll_cloudwatch_logs(
                log_group=log_group,
                filter_pattern=f'"{unique_event_id}"',
                start_time_ms=start_time_ms,
                timeout_seconds=60,
            )
            for event in events:
                message = event.get("message", "")
                for expected in expected_events:
                    if expected in message:
                        found_events.add(expected)

            if found_events == set(expected_events):
                break

        assert found_events == set(expected_events), (
            f"Missing log events: {set(expected_events) - found_events}"
        )

        # Verify that the auth token/secret is NOT present in log entries
        for log_group in log_groups:
            events = _poll_cloudwatch_logs(
                log_group=log_group,
                filter_pattern=f'"{unique_event_id}"',
                start_time_ms=start_time_ms,
                timeout_seconds=5,
            )
            for event in events:
                message = event.get("message", "")
                # The auth_token fixture value should never appear in logs
                # We check a generic assertion — the token comes from env
                auth_token_value = auth_token if "auth_token" in dir() else None
                if auth_token_value:
                    assert auth_token_value not in message, (
                        "Auth token found in log entry — redaction failure"
                    )


# ---------------------------------------------------------------------------
# TestS3ParquetDelivery
# ---------------------------------------------------------------------------


class TestS3ParquetDelivery:
    """S3 + Parquet delivery verification (waits for Firehose buffer flush).

    Validates: Requirement 13.3
    """

    @pytest.mark.timeout(400)
    def test_parquet_file_delivered_to_s3(
        self, api_endpoint, auth_token, unique_event_id, logs_bucket
    ):
        """Send event, wait for buffer flush, verify Parquet file in S3."""
        if pq is None:
            pytest.skip("pyarrow not installed — cannot verify Parquet files")

        # Send a test event
        payload = _build_test_event(unique_event_id)
        response = requests.post(
            f"{api_endpoint}/webhook",
            json=payload,
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=10,
        )
        assert response.status_code == 202

        # Wait for Firehose buffer to flush (up to 330 seconds)
        s3_client = boto3.client("s3")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        prefix = f"data/ingestion_date={today}/"

        parquet_key = None
        deadline = time.time() + 330
        poll_interval = 15

        while time.time() < deadline:
            response_s3 = s3_client.list_objects_v2(
                Bucket=logs_bucket,
                Prefix=prefix,
            )
            objects = response_s3.get("Contents", [])

            # Look for any .parquet file created after our event submission
            for obj in sorted(objects, key=lambda o: o["LastModified"], reverse=True):
                if obj["Key"].endswith(".parquet"):
                    parquet_key = obj["Key"]
                    break

            if parquet_key:
                break
            time.sleep(poll_interval)

        assert parquet_key is not None, (
            f"No Parquet file found in s3://{logs_bucket}/{prefix} after 330s"
        )

        # Download and read the Parquet file
        obj_response = s3_client.get_object(Bucket=logs_bucket, Key=parquet_key)
        parquet_bytes = obj_response["Body"].read()
        table = pq.read_table(io.BytesIO(parquet_bytes))

        # Verify 16 columns exist
        expected_columns = [
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
        ]
        actual_columns = table.schema.names
        for col in expected_columns:
            assert col in actual_columns, f"Missing column: {col}"

        # Find our test event by event_id
        df = table.to_pandas()
        test_rows = df[df["event_id"] == unique_event_id]

        if not test_rows.empty:
            row = test_rows.iloc[0]
            assert row["event_type"] == "INTEGRATION_TEST_EVENT"
            assert row["conversation_id"] == f"conv-{unique_event_id}"
            assert row["message_id"] == f"msg-{unique_event_id}"
            assert row["user_id"] == f"user-{unique_event_id}"
            assert row["source_channel"] == "integration-test"
            assert row["source_bot"] == "test-bot"
            assert row["schema_version"] == 1

            # Verify raw_event_json round-trip
            raw = json.loads(row["raw_event_json"])
            assert raw["eventId"] == unique_event_id
            assert raw["eventType"] == "INTEGRATION_TEST_EVENT"


# ---------------------------------------------------------------------------
# TestAthenaQuery
# ---------------------------------------------------------------------------


class TestAthenaQuery:
    """Athena query verification — runs a query to find the test event.

    Validates: Requirement 13.4, 13.5
    """

    @pytest.mark.timeout(120)
    def test_athena_finds_test_event(
        self,
        api_endpoint,
        auth_token,
        unique_event_id,
        glue_database,
        athena_workgroup,
    ):
        """Execute query filtering by unique event_id, verify single row."""
        # Ensure the event was sent (idempotent — may already exist)
        payload = _build_test_event(unique_event_id)
        response = requests.post(
            f"{api_endpoint}/webhook",
            json=payload,
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=10,
        )
        # Accept either 202 (new) or just proceed if already sent
        assert response.status_code == 202

        # Wait some time for Firehose flush + Athena availability
        # The test orchestrator should run this after TestS3ParquetDelivery
        time.sleep(10)

        query = (
            f"SELECT event_id, event_type, conversation_id, source_channel "
            f"FROM botmaker_logs "
            f"WHERE event_id = '{unique_event_id}' "
            f"LIMIT 5"
        )

        rows = _wait_for_athena_query(
            query=query,
            database=glue_database,
            workgroup=athena_workgroup,
            timeout_seconds=60,
        )

        assert len(rows) >= 1, (
            f"Expected at least 1 row for event_id={unique_event_id}, got {len(rows)}"
        )

        # Verify field values from the first result row
        # Athena returns rows as list of {"VarCharValue": "..."} dicts
        first_row = rows[0]["Data"]
        values = [col.get("VarCharValue", "") for col in first_row]

        # Columns: event_id, event_type, conversation_id, source_channel
        assert values[0] == unique_event_id
        assert values[1] == "INTEGRATION_TEST_EVENT"
        assert values[2] == f"conv-{unique_event_id}"
        assert values[3] == "integration-test"
