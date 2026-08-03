"""Structured JSON logging with sensitive value redaction.

Emits JSON lines to stdout for CloudWatch ingestion.
Supports correlation IDs, event types, and recursive redaction
of sensitive fields (tokens, secrets, API keys).

Requirements: 9.2, 9.4, 9.5, 16.2
"""

import copy
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional


# Patterns that indicate a sensitive key (case-insensitive)
_SENSITIVE_KEY_PATTERNS = re.compile(
    r"(authorization|token|secret|password|api_key|api-key|apikey|bearer|credential)",
    re.IGNORECASE,
)

# Supported event types for the ingestion pipeline
EVENT_TYPE_INGESTION_RECEIVED = "INGESTION_RECEIVED"
EVENT_TYPE_INGESTION_VALIDATED = "INGESTION_VALIDATED"
EVENT_TYPE_FIREHOSE_ACCEPTED = "FIREHOSE_ACCEPTED"

_REDACTED = "[REDACTED]"


def redact_sensitive(data: Any) -> Any:
    """Recursively redact sensitive values from a data structure.

    Walks dicts and lists, replacing values whose keys match
    sensitive patterns (authorization, token, secret, password,
    api_key, bearer) with '[REDACTED]'.

    Uses case-insensitive matching on key names.
    Returns a sanitized copy — does NOT mutate the input.

    Args:
        data: Any Python data structure (dict, list, scalar).

    Returns:
        A deep copy with sensitive values replaced by '[REDACTED]'.
    """
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if isinstance(key, str) and _SENSITIVE_KEY_PATTERNS.search(key):
                result[key] = _REDACTED
            else:
                result[key] = redact_sensitive(value)
        return result
    elif isinstance(data, list):
        return [redact_sensitive(item) for item in data]
    else:
        return copy.deepcopy(data) if isinstance(data, (dict, list)) else data


class StructuredLogger:
    """Structured JSON logger for the ingestion pipeline.

    Emits JSON-line log entries to stdout with consistent fields:
    timestamp, level, correlation_id, event_type, outcome.

    Optional fields: ingestion_id, source_event_type, duration_ms,
    error_message.

    Raw request bodies are NEVER logged. Only metadata fields are emitted.
    All sensitive values are redacted before logging.
    """

    def __init__(self, correlation_id: str, log_payload: bool = False):
        """Initialize the structured logger.

        Args:
            correlation_id: Request-scoped ID (typically API Gateway request ID)
                that stays consistent across all log entries for a single request.
            log_payload: When True, log sanitized metadata about the payload
                (event_type, field presence). When False, don't log any payload details.
        """
        self._correlation_id = correlation_id
        self._log_payload = log_payload

    @property
    def correlation_id(self) -> str:
        """The correlation ID for this logger instance."""
        return self._correlation_id

    def log(
        self,
        level: str,
        event_type: str,
        outcome: str,
        *,
        ingestion_id: Optional[str] = None,
        source_event_type: Optional[str] = None,
        duration_ms: Optional[float] = None,
        error_message: Optional[str] = None,
        payload_metadata: Optional[dict] = None,
    ) -> str:
        """Emit a structured JSON log entry.

        Args:
            level: Log level (INFO, WARNING, ERROR).
            event_type: Processing stage identifier
                (INGESTION_RECEIVED, INGESTION_VALIDATED, FIREHOSE_ACCEPTED).
            outcome: Processing result (success or failure).
            ingestion_id: UUID of the ingestion record (optional).
            source_event_type: The event_type extracted from the webhook payload (optional).
            duration_ms: Processing duration in milliseconds (optional).
            error_message: Error description (optional). Will be redacted if sensitive.
            payload_metadata: Metadata about the payload (optional).
                Only included when log_payload is True.
                Must NOT contain raw body content.

        Returns:
            The JSON string that was logged (useful for testing).
        """
        entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level": level,
            "correlation_id": self._correlation_id,
            "event_type": event_type,
            "outcome": outcome,
        }

        if ingestion_id is not None:
            entry["ingestion_id"] = ingestion_id

        if source_event_type is not None:
            entry["source_event_type"] = source_event_type

        if duration_ms is not None:
            entry["duration_ms"] = duration_ms

        if error_message is not None:
            # Redact any sensitive content that might leak into error messages
            entry["error_message"] = redact_sensitive(error_message) if isinstance(error_message, (dict, list)) else error_message

        if self._log_payload and payload_metadata is not None:
            # Redact sensitive fields from payload metadata before logging
            entry["payload_metadata"] = redact_sensitive(payload_metadata)

        # Final safety pass: redact the entire entry to catch any leaked secrets
        safe_entry = redact_sensitive(entry)

        log_line = json.dumps(safe_entry, default=str)
        print(log_line, flush=True)
        return log_line

    def info(
        self,
        event_type: str,
        outcome: str = "success",
        **kwargs: Any,
    ) -> str:
        """Emit an INFO-level log entry."""
        return self.log("INFO", event_type, outcome, **kwargs)

    def warning(
        self,
        event_type: str,
        outcome: str = "failure",
        **kwargs: Any,
    ) -> str:
        """Emit a WARNING-level log entry."""
        return self.log("WARNING", event_type, outcome, **kwargs)

    def error(
        self,
        event_type: str,
        outcome: str = "failure",
        **kwargs: Any,
    ) -> str:
        """Emit an ERROR-level log entry."""
        return self.log("ERROR", event_type, outcome, **kwargs)


class Timer:
    """Context manager for measuring duration_ms.

    Usage:
        with Timer() as t:
            do_work()
        logger.info(..., duration_ms=t.duration_ms)
    """

    def __init__(self) -> None:
        self._start: float = 0
        self._end: float = 0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self._end = time.perf_counter()

    @property
    def duration_ms(self) -> float:
        """Elapsed time in milliseconds."""
        return round((self._end - self._start) * 1000, 2)
