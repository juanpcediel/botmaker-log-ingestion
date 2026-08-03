"""Parquet schema round-trip tests.

Validates that Normalized_Records written to Parquet using the Glue table
schema can be read back with identical field values, types, and null handling.

Requirements: 11.1, 11.2, 11.3, 11.5
"""

from __future__ import annotations

import json
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Expected Glue table schema (16 fields + ingestion_date partition key)
# ---------------------------------------------------------------------------

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("ingestion_id", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("request_id", pa.string(), nullable=True),
        pa.field("received_at", pa.string(), nullable=False),
        pa.field("schema_version", pa.int32(), nullable=False),
        pa.field("environment", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("conversation_id", pa.string(), nullable=True),
        pa.field("message_id", pa.string(), nullable=True),
        pa.field("user_id", pa.string(), nullable=True),
        pa.field("order_form_id", pa.string(), nullable=True),
        pa.field("source_channel", pa.string(), nullable=True),
        pa.field("source_bot", pa.string(), nullable=True),
        pa.field("severity", pa.string(), nullable=True),
        pa.field("error_code", pa.string(), nullable=True),
        pa.field("raw_event_json", pa.string(), nullable=False),
        pa.field("ingestion_date", pa.string(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# Sample records
# ---------------------------------------------------------------------------

ALL_POPULATED_RECORD: dict = {
    "ingestion_id": str(uuid.uuid4()),
    "event_id": "evt-001",
    "request_id": "req-abc-123",
    "received_at": "2024-06-15T14:30:00.123Z",
    "schema_version": 1,
    "environment": "dev",
    "event_type": "MESSAGE_RECEIVED",
    "conversation_id": "conv-xyz",
    "message_id": "msg-456",
    "user_id": "usr-789",
    "order_form_id": "of-012",
    "source_channel": "whatsapp",
    "source_bot": "bot-main",
    "severity": "INFO",
    "error_code": "E001",
    "raw_event_json": json.dumps(
        {"eventId": "evt-001", "eventType": "MESSAGE_RECEIVED"},
        ensure_ascii=False,
        separators=(",", ":"),
    ),
    "ingestion_date": "2024-06-15",
}

ALL_NULL_OPTIONAL_RECORD: dict = {
    "ingestion_id": str(uuid.uuid4()),
    "event_id": "evt-002",
    "request_id": None,
    "received_at": "2024-06-15T14:31:00.456Z",
    "schema_version": 1,
    "environment": "prod",
    "event_type": "UNKNOWN",
    "conversation_id": None,
    "message_id": None,
    "user_id": None,
    "order_form_id": None,
    "source_channel": None,
    "source_bot": None,
    "severity": None,
    "error_code": None,
    "raw_event_json": json.dumps({}, ensure_ascii=False, separators=(",", ":")),
    "ingestion_date": "2024-06-15",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _records_to_table(records: list[dict]) -> pa.Table:
    """Convert a list of record dicts into a PyArrow table using EXPECTED_SCHEMA."""
    arrays = []
    for field in EXPECTED_SCHEMA:
        values = [record[field.name] for record in records]
        arrays.append(pa.array(values, type=field.type))
    return pa.table(arrays, schema=EXPECTED_SCHEMA)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGlueSchemaFieldTypes:
    """Validate each field type maps correctly to Parquet column type from Glue definition."""

    def test_schema_has_17_fields(self):
        """Schema defines exactly 16 Normalized_Record fields + ingestion_date."""
        assert len(EXPECTED_SCHEMA) == 17

    @pytest.mark.parametrize(
        "field_name,expected_type",
        [
            ("ingestion_id", pa.string()),
            ("event_id", pa.string()),
            ("request_id", pa.string()),
            ("received_at", pa.string()),
            ("schema_version", pa.int32()),
            ("environment", pa.string()),
            ("event_type", pa.string()),
            ("conversation_id", pa.string()),
            ("message_id", pa.string()),
            ("user_id", pa.string()),
            ("order_form_id", pa.string()),
            ("source_channel", pa.string()),
            ("source_bot", pa.string()),
            ("severity", pa.string()),
            ("error_code", pa.string()),
            ("raw_event_json", pa.string()),
            ("ingestion_date", pa.string()),
        ],
    )
    def test_field_type_matches_glue_definition(self, field_name: str, expected_type):
        """Each field in the schema maps to the correct Parquet type."""
        schema_field = EXPECTED_SCHEMA.field(field_name)
        assert schema_field.type == expected_type, (
            f"Field '{field_name}' type mismatch: "
            f"expected {expected_type}, actual {schema_field.type}"
        )


class TestParquetRoundTrip:
    """Write Parquet file locally, read back, assert schema match, row count, and field values."""

    def test_round_trip_all_populated(self, tmp_path):
        """Round-trip with all fields populated preserves schema and values."""
        records = [ALL_POPULATED_RECORD]
        table = _records_to_table(records)
        parquet_path = tmp_path / "test_all_populated.parquet"

        pq.write_table(table, str(parquet_path))
        read_table = pq.read_table(str(parquet_path))

        # Schema match
        assert read_table.schema.equals(EXPECTED_SCHEMA), (
            f"Schema mismatch.\nExpected: {EXPECTED_SCHEMA}\nActual: {read_table.schema}"
        )

        # Row count
        assert read_table.num_rows == 1

        # Field value equality
        for field in EXPECTED_SCHEMA:
            written_value = ALL_POPULATED_RECORD[field.name]
            read_value = read_table.column(field.name)[0].as_py()
            assert read_value == written_value, (
                f"Field '{field.name}' value mismatch: "
                f"expected {written_value!r}, actual {read_value!r}"
            )

    def test_round_trip_all_null_optional(self, tmp_path):
        """Round-trip with all optional fields as None preserves null values."""
        records = [ALL_NULL_OPTIONAL_RECORD]
        table = _records_to_table(records)
        parquet_path = tmp_path / "test_all_null_optional.parquet"

        pq.write_table(table, str(parquet_path))
        read_table = pq.read_table(str(parquet_path))

        # Schema match
        assert read_table.schema.equals(EXPECTED_SCHEMA), (
            f"Schema mismatch.\nExpected: {EXPECTED_SCHEMA}\nActual: {read_table.schema}"
        )

        # Row count
        assert read_table.num_rows == 1

        # Field value equality (including None for optional fields)
        for field in EXPECTED_SCHEMA:
            written_value = ALL_NULL_OPTIONAL_RECORD[field.name]
            read_value = read_table.column(field.name)[0].as_py()
            assert read_value == written_value, (
                f"Field '{field.name}' value mismatch: "
                f"expected {written_value!r}, actual {read_value!r}"
            )

    def test_round_trip_multiple_records(self, tmp_path):
        """Round-trip with multiple records preserves row count and all values."""
        records = [ALL_POPULATED_RECORD, ALL_NULL_OPTIONAL_RECORD]
        table = _records_to_table(records)
        parquet_path = tmp_path / "test_multiple.parquet"

        pq.write_table(table, str(parquet_path))
        read_table = pq.read_table(str(parquet_path))

        # Schema match
        assert read_table.schema.equals(EXPECTED_SCHEMA)

        # Row count
        assert read_table.num_rows == 2

        # Verify each row
        for row_idx, record in enumerate(records):
            for field in EXPECTED_SCHEMA:
                read_value = read_table.column(field.name)[row_idx].as_py()
                expected_value = record[field.name]
                assert read_value == expected_value, (
                    f"Row {row_idx}, field '{field.name}' value mismatch: "
                    f"expected {expected_value!r}, actual {read_value!r}"
                )

    def test_raw_event_json_deserializable(self, tmp_path):
        """raw_event_json can be deserialized back to valid JSON after round-trip."""
        records = [ALL_POPULATED_RECORD, ALL_NULL_OPTIONAL_RECORD]
        table = _records_to_table(records)
        parquet_path = tmp_path / "test_json_deser.parquet"

        pq.write_table(table, str(parquet_path))
        read_table = pq.read_table(str(parquet_path))

        for row_idx in range(read_table.num_rows):
            raw_json_str = read_table.column("raw_event_json")[row_idx].as_py()
            # Must be deserializable
            parsed = json.loads(raw_json_str)
            # Must produce a dict (JSON object)
            assert isinstance(parsed, dict), (
                f"Row {row_idx}: raw_event_json deserialized to "
                f"{type(parsed).__name__}, expected dict"
            )
            # Must match the original JSON content
            original_parsed = json.loads(records[row_idx]["raw_event_json"])
            assert parsed == original_parsed, (
                f"Row {row_idx}: raw_event_json content mismatch after round-trip"
            )


class TestSchemaMismatchDetection:
    """Fail with assertion identifying mismatched field and expected vs actual type."""

    def test_detects_wrong_field_type(self, tmp_path):
        """Writing with wrong type and reading back detects schema mismatch."""
        # Create a table with schema_version as int64 instead of int32
        wrong_schema = pa.schema(
            [
                pa.field("ingestion_id", pa.string(), nullable=False),
                pa.field("event_id", pa.string(), nullable=False),
                pa.field("request_id", pa.string(), nullable=True),
                pa.field("received_at", pa.string(), nullable=False),
                pa.field("schema_version", pa.int64(), nullable=False),  # WRONG
                pa.field("environment", pa.string(), nullable=False),
                pa.field("event_type", pa.string(), nullable=False),
                pa.field("conversation_id", pa.string(), nullable=True),
                pa.field("message_id", pa.string(), nullable=True),
                pa.field("user_id", pa.string(), nullable=True),
                pa.field("order_form_id", pa.string(), nullable=True),
                pa.field("source_channel", pa.string(), nullable=True),
                pa.field("source_bot", pa.string(), nullable=True),
                pa.field("severity", pa.string(), nullable=True),
                pa.field("error_code", pa.string(), nullable=True),
                pa.field("raw_event_json", pa.string(), nullable=False),
                pa.field("ingestion_date", pa.string(), nullable=False),
            ]
        )

        arrays = []
        for field in wrong_schema:
            values = [ALL_POPULATED_RECORD[field.name]]
            arrays.append(pa.array(values, type=field.type))
        wrong_table = pa.table(arrays, schema=wrong_schema)

        parquet_path = tmp_path / "test_wrong_type.parquet"
        pq.write_table(wrong_table, str(parquet_path))
        read_table = pq.read_table(str(parquet_path))

        # Validate each field type against expected schema and detect mismatch
        mismatches = []
        for expected_field in EXPECTED_SCHEMA:
            actual_field = read_table.schema.field(expected_field.name)
            if actual_field.type != expected_field.type:
                mismatches.append(
                    f"Field '{expected_field.name}': "
                    f"expected {expected_field.type}, actual {actual_field.type}"
                )

        assert len(mismatches) > 0, "Expected at least one type mismatch"
        assert any("schema_version" in m for m in mismatches), (
            f"Expected schema_version mismatch, got: {mismatches}"
        )
