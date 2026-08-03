"""Environment-based configuration module for the Botmaker log ingestion Lambda."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


# --- Constants ---

SCHEMA_VERSION: int = 1
MAX_BODY_SIZE_BYTES: int = 1_048_576  # 1 MB
MAX_RAW_PAYLOAD_BYTES: int = 6_291_456  # 6 MB
SECRET_CACHE_TTL: int = 300  # seconds
SECRET_TIMEOUT: int = 5  # seconds


@dataclass(frozen=True)
class Config:
    """Application configuration loaded from environment variables.

    Required variables (no defaults):
        ENVIRONMENT — deployment environment name (e.g. dev, prod).
        FIREHOSE_STREAM_NAME — Firehose delivery stream name.
        SECRET_ID — Secrets Manager secret ARN/name for the webhook token.

    Optional variables:
        LOG_LEVEL — Python logging level (default: INFO).
        LOG_PAYLOAD — whether to log request payloads (default: false, case-insensitive).
    """

    environment: str
    firehose_stream_name: str
    secret_id: str
    log_level: str = "INFO"
    log_payload: bool = False

    # Class-level constants (accessible via instance or class)
    schema_version: int = field(default=SCHEMA_VERSION, init=False, repr=False)
    max_body_size_bytes: int = field(default=MAX_BODY_SIZE_BYTES, init=False, repr=False)
    max_raw_payload_bytes: int = field(default=MAX_RAW_PAYLOAD_BYTES, init=False, repr=False)
    secret_cache_ttl: int = field(default=SECRET_CACHE_TTL, init=False, repr=False)
    secret_timeout: int = field(default=SECRET_TIMEOUT, init=False, repr=False)


def _parse_log_payload(value: str) -> bool:
    """Parse LOG_PAYLOAD env var as a boolean (case-insensitive)."""
    return value.strip().lower() == "true"


def _build_config() -> Config:
    """Build a Config instance from current environment variables."""
    environment = os.environ.get("ENVIRONMENT")
    if not environment:
        raise EnvironmentError("Required environment variable ENVIRONMENT is not set")

    firehose_stream_name = os.environ.get("FIREHOSE_STREAM_NAME")
    if not firehose_stream_name:
        raise EnvironmentError("Required environment variable FIREHOSE_STREAM_NAME is not set")

    secret_id = os.environ.get("SECRET_ID")
    if not secret_id:
        raise EnvironmentError("Required environment variable SECRET_ID is not set")

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_payload = _parse_log_payload(os.environ.get("LOG_PAYLOAD", "false"))

    return Config(
        environment=environment,
        firehose_stream_name=firehose_stream_name,
        secret_id=secret_id,
        log_level=log_level,
        log_payload=log_payload,
    )


@lru_cache(maxsize=1)
def get_config() -> Config:
    """Return the singleton Config instance, built once from environment variables.

    Uses lru_cache to ensure the config is only read once per Lambda execution
    environment (cold start). Call get_config.cache_clear() in tests to reset.
    """
    return _build_config()
