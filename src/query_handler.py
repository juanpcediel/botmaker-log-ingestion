"""Query handler for GET endpoints — health, lookup by ID, and listing.

Endpoints:
    GET /webhook/health             → service health check
    GET /webhook/{ingestion_id}     → lookup single event (CloudWatch + Athena)
    GET /webhook?date=YYYY-MM-DD    → list events by date (Athena, paginated)
    GET /webhook?date=...&event_type=X → filtered list (Athena)
"""

from __future__ import annotations

import json
import time
from typing import Any, Optional

import boto3

from config import get_config


_athena_client = boto3.client("athena")
_logs_client = boto3.client("logs")

# Maximum records returned per list request
MAX_PAGE_SIZE = 100


def _response(status_code: int, body: dict) -> dict:
    """Build an API Gateway Lambda proxy response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, default=str),
    }


# =============================================================================
# GET /webhook/health
# =============================================================================


def handle_health() -> dict:
    """Return service health status."""
    config = get_config()
    return _response(200, {
        "status": "healthy",
        "environment": config.environment,
        "firehose_stream": config.firehose_stream_name,
    })


# =============================================================================
# GET /webhook/{ingestion_id}
# =============================================================================


def handle_get_by_id(ingestion_id: str) -> dict:
    """Lookup a single event by ingestion_id.

    Strategy:
    1. Check CloudWatch Logs for FIREHOSE_ACCEPTED (confirms processing)
    2. Query Athena for the stored Parquet record (confirms S3 delivery)
    """
    if not ingestion_id or len(ingestion_id) < 10:
        return _response(400, {
            "error": "Bad Request",
            "message": "Invalid ingestion_id format",
        })

    config = get_config()

    # Step 1: Check CloudWatch for processing confirmation
    cloudwatch_status = _check_cloudwatch(ingestion_id, config)

    # Step 2: Query Athena for stored record
    athena_record = _query_athena_by_id(ingestion_id, config)

    if athena_record:
        return _response(200, {
            "status": "stored",
            "ingestion_id": ingestion_id,
            "stored_at": athena_record.get("ingestion_date", "unknown"),
            "record": athena_record,
        })
    elif cloudwatch_status == "accepted":
        return _response(200, {
            "status": "pending",
            "ingestion_id": ingestion_id,
            "message": "Event accepted but not yet delivered to storage. Firehose buffers ~5 minutes.",
        })
    else:
        return _response(404, {
            "status": "not_found",
            "ingestion_id": ingestion_id,
            "message": "No event found with this ingestion_id",
        })


def _check_cloudwatch(ingestion_id: str, config) -> Optional[str]:
    """Check CloudWatch Logs for FIREHOSE_ACCEPTED with the given ingestion_id."""
    try:
        log_group = f"/aws/lambda/botmaker-log-ingestion-{config.environment}"
        # Search last 10 minutes
        start_time = int((time.time() - 600) * 1000)

        response = _logs_client.filter_log_events(
            logGroupName=log_group,
            filterPattern=f'"{ingestion_id}" "FIREHOSE_ACCEPTED"',
            startTime=start_time,
            limit=5,
        )

        if response.get("events"):
            return "accepted"
    except Exception:
        pass

    return None


def _query_athena_by_id(ingestion_id: str, config) -> Optional[dict]:
    """Query Athena for a specific event by ingestion_id."""
    database = f"botmaker_log_ingestion_{config.environment}"
    workgroup = f"botmaker-log-ingestion-{config.environment}"

    query = (
        f"SELECT * FROM botmaker_logs "
        f"WHERE ingestion_id = '{ingestion_id}' "
        f"LIMIT 1"
    )

    return _execute_athena_query_single(query, database, workgroup)


# =============================================================================
# GET /webhook?date=YYYY-MM-DD[&event_type=X][&conversation_id=X][&limit=N]
# =============================================================================


def handle_list(query_params: dict) -> dict:
    """List events by date with optional filters."""
    date = query_params.get("date")
    if not date:
        return _response(400, {
            "error": "Bad Request",
            "message": "Query parameter 'date' is required (format: YYYY-MM-DD)",
        })

    # Validate date format
    if len(date) != 10 or date[4] != "-" or date[7] != "-":
        return _response(400, {
            "error": "Bad Request",
            "message": "Invalid date format. Use YYYY-MM-DD",
        })

    config = get_config()
    database = f"botmaker_log_ingestion_{config.environment}"
    workgroup = f"botmaker-log-ingestion-{config.environment}"

    # Build query with filters
    limit = min(int(query_params.get("limit", "100")), MAX_PAGE_SIZE)
    conditions = [f"ingestion_date = '{date}'"]

    # Optional filters
    event_type = query_params.get("event_type")
    if event_type:
        conditions.append(f"event_type = '{event_type}'")

    conversation_id = query_params.get("conversation_id")
    if conversation_id:
        conditions.append(f"conversation_id = '{conversation_id}'")

    source_channel = query_params.get("source_channel")
    if source_channel:
        conditions.append(f"source_channel = '{source_channel}'")

    where_clause = " AND ".join(conditions)
    query = f"SELECT * FROM botmaker_logs WHERE {where_clause} ORDER BY received_at DESC LIMIT {limit}"

    records = _execute_athena_query_list(query, database, workgroup)

    if records is None:
        return _response(503, {
            "error": "Service Unavailable",
            "message": "Query execution failed. Try again later.",
        })

    return _response(200, {
        "status": "success",
        "date": date,
        "filters": {k: v for k, v in {
            "event_type": event_type,
            "conversation_id": conversation_id,
            "source_channel": source_channel,
        }.items() if v},
        "count": len(records),
        "records": records,
    })


# =============================================================================
# Athena query helpers
# =============================================================================


def _execute_athena_query_single(query: str, database: str, workgroup: str) -> Optional[dict]:
    """Execute an Athena query and return a single row as a dict, or None."""
    records = _execute_athena_query_list(query, database, workgroup)
    if records and len(records) > 0:
        return records[0]
    return None


def _execute_athena_query_list(query: str, database: str, workgroup: str) -> Optional[list[dict]]:
    """Execute an Athena query and return results as a list of dicts."""
    try:
        start_response = _athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            WorkGroup=workgroup,
        )
        query_execution_id = start_response["QueryExecutionId"]

        # Poll for completion (max 30 seconds)
        deadline = time.time() + 30
        while time.time() < deadline:
            status = _athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            state = status["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                break
            elif state in ("FAILED", "CANCELLED"):
                return None

            time.sleep(1)
        else:
            return None  # Timeout

        # Fetch results
        results = _athena_client.get_query_results(
            QueryExecutionId=query_execution_id
        )

        rows = results["ResultSet"]["Rows"]
        if len(rows) <= 1:
            return []

        # First row is the header
        headers = [col["VarCharValue"] for col in rows[0]["Data"]]
        records = []
        for row in rows[1:]:
            record = {}
            for i, col in enumerate(row["Data"]):
                value = col.get("VarCharValue")
                record[headers[i]] = value
            records.append(record)

        return records

    except Exception:
        return None
