"""Firehose PutRecord wrapper with error handling.

Serializes Normalized_Records as single-line JSON UTF-8 and delivers
them to an Amazon Data Firehose delivery stream via PutRecord.

Uses boto3 adaptive retry mode with max 3 attempts. On retry exhaustion,
raises FirehoseDeliveryError which maps to HTTP 503.

Requirements: 5.1, 5.12, 5.13
"""

from __future__ import annotations

import json

from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError


# boto3 client configuration for Firehose with adaptive retry
FIREHOSE_BOTO_CONFIG = BotoConfig(
    retries={"mode": "adaptive", "max_attempts": 3}
)


class FirehoseDeliveryError(Exception):
    """Raised when PutRecord fails after all SDK retries are exhausted.

    This exception signals that the Firehose delivery stream could not
    accept the record. The Lambda handler should catch this and return
    HTTP 503 Service Unavailable to the caller.
    """

    pass


def deliver_record(record: dict, stream_name: str, client) -> str:
    """Serialize and deliver a normalized record to Firehose.

    Serializes the record as compact single-line JSON (UTF-8) and calls
    PutRecord on the specified Firehose delivery stream.

    Args:
        record: The 16-field Normalized_Record dict.
        stream_name: Firehose delivery stream name.
        client: boto3 Firehose client instance (should be configured
                with FIREHOSE_BOTO_CONFIG for adaptive retry).

    Returns:
        The RecordId from Firehose on success.

    Raises:
        FirehoseDeliveryError: If PutRecord fails after retries.
    """
    try:
        data = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        data_bytes = data.encode("utf-8")

        response = client.put_record(
            DeliveryStreamName=stream_name,
            Record={"Data": data_bytes},
        )

        return response["RecordId"]

    except (ClientError, BotoCoreError) as exc:
        raise FirehoseDeliveryError(
            f"PutRecord failed for stream '{stream_name}': {exc}"
        ) from exc
