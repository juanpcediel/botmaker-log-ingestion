"""Property-based tests for Parquet schema round-trip.

Properties tested:
  - Property 10: Parquet Schema Round-Trip

Validates that any valid Normalized_Record written to Parquet using the
Glue table schema can be read back with identical field values, including
null handling for optional fields.

Minimum 100 Hypothesis examples.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid

os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("FIREHOSE_STREAM_NAME", "test-stream")
os.environ.setdefault("SECRET_ID", "test-secret")

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import given, settings
from hypothesis import strategies as st

# Import the canonical schema from the schema test module
from tests.schema.test_parquet_schema import EXPECTED_SCHEMA

# ---------------------------------------------------------------------------
# Hypothesis strategies for valid Normalized_Records
# ---------------------------------------------------------------------------

# ISO-8601 UTC timestamps with ms precision
iso8601_timestamps = st.builds(
    lambda y, mo, d, h, mi, s, ms: f"{y:04d}-{mo:02d}-{d:02d}T{h:02d}:{mi:02d}:{s:02d}.{ms:03d}Z",
    y=st.integers(min_value=2020, max_value=2030),
    mo=st.integers(min_value=1, max_value=12),
    d=st.integers(min_value=1, max_value=28),
    h=st.integers(min_value=0, max_value=23),
    mi=st.integers(min_value=0, max_value=59),
    s=st.integers(min_value=0, max_value=59),
    ms=st.integers(min_value=0, max_value=999),
)

# UUID v4 strings
uuid4_strings = st.builds(lambda: str(uuid.uuid4()))

# Non-empty text for required string fields
non_empty_text = st.text(min_size=1, max_size=64).filter(lambda s: s.strip() != "")

# Optional string fields (nullable)
optional_text = st.one_of(st.none(), st.text(min_size=1, max_size=64))

# Environment names
environments = st.sampled_from(["dev", "staging", "prod", "test"])

# Event types
event_types = st.one_of(
    st.sampled_from([
        "MESSAGE_RECEIVED", "MESSAGE_SENT", "PAYMENT_LINK_EVENT",
        "ORDER_EVENT", "UNKNOWN", "ERROR",
    ]),
    st.text(min_size=1, max_size=32).filter(lambda s: s.strip() != ""),
)

# Ingestion date (yyyy-MM-dd)
ingestion_dates = st.builds(
    lambda y, mo, d: f"{y:04d}-{mo:02d}-{d:02d}",
    y=st.integers(min_value=2020, max_value=2030),
    mo=st.integers(min_value=1, max_value=12),
    d=st.integers(min_value=1, max_value=28),
)

# Raw event JSON — generate a small dict and serialize it
raw_event_dicts = st.fixed_dictionaries({}, optional={
    "eventId": st.text(min_size=1, max_size=32),
    "eventType": st.text(min_size=1, max_size=32),
    "data": st.text(max_size=64),
})


# Complete Normalized_Record strategy (including ingestion_date for Parquet)
normalized_records = st.fixed_dictionaries({
    "ingestion_id": uuid4_strings,
    "event_id": non_empty_text,
    "request_id": optional_text,
    "received_at": iso8601_timestamps,
    "schema_version": st.just(1),
    "environment": environments,
    "event_type": event_types,
    "conversation_id": optional_text,
    "message_id": optional_text,
    "user_id": optional_text,
    "order_form_id": optional_text,
    "source_channel": optional_text,
    "source_bot": optional_text,
    "severity": optional_text,
    "error_code": optional_text,
    "raw_event_json": raw_event_dicts.map(
        lambda d: json.dumps(d, ensure_ascii=False, separators=(",", ":"))
    ),
    "ingestion_date": ingestion_dates,
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _record_to_table(record: dict) -> pa.Table:
    """Convert a single record dict into a PyArrow table using EXPECTED_SCHEMA."""
    arrays = []
    for field in EXPECTED_SCHEMA:
        value = record[field.name]
        arrays.append(pa.array([value], type=field.type))
    return pa.table(arrays, schema=EXPECTED_SCHEMA)


# ---------------------------------------------------------------------------
# Property 10: Parquet Schema Round-Trip
# ---------------------------------------------------------------------------


@settings(max_examples=100, deadline=10000)
@given(record=normalized_records)
def test_parquet_schema_round_trip(record, tmp_path_factory):
    """Writing a valid Normalized_Record to Parquet and reading it back
    preserves all field values identically, including null values for
    optional absent fields.

    **Validates: Requirements 11.4**
    """
    # Write to Parquet
    table = _record_to_table(record)
    tmp_dir = tmp_path_factory.mktemp("parquet")
    parquet_path = str(tmp_dir / "test.parquet")
    pq.write_table(table, parquet_path)

    # Read back
    read_table = pq.read_table(parquet_path)

    # Verify schema matches
    assert read_table.schema.equals(EXPECTED_SCHEMA), (
        f"Schema mismatch.\nExpected: {EXPECTED_SCHEMA}\nActual: {read_table.schema}"
    )

    # Verify row count
    assert read_table.num_rows == 1

    # Verify field-by-field equality including nulls
    for field in EXPECTED_SCHEMA:
        expected_value = record[field.name]
        actual_value = read_table.column(field.name)[0].as_py()
        assert actual_value == expected_value, (
            f"Field '{field.name}' mismatch: "
            f"expected {expected_value!r}, actual {actual_value!r}"
        )
