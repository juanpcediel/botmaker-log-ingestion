"""Unit tests for src/config.py — environment-based configuration module.

Validates:
- Configuration class reads all expected environment variables
- Required variables raise EnvironmentError when missing
- Optional variables have correct defaults
- Constants have correct values per spec
- get_config singleton behavior (lru_cache)
- LOG_PAYLOAD parsing as boolean

Requirements: 2.4, 4.4, 16.1, 16.5
"""

import os

import pytest

from config import (
    MAX_BODY_SIZE_BYTES,
    MAX_RAW_PAYLOAD_BYTES,
    SCHEMA_VERSION,
    SECRET_CACHE_TTL,
    SECRET_TIMEOUT,
    Config,
    get_config,
    _build_config,
    _parse_log_payload,
)


class TestConstants:
    """Verify module-level constants match spec values."""

    def test_schema_version_is_1(self):
        assert SCHEMA_VERSION == 1

    def test_max_body_size_is_1mb(self):
        assert MAX_BODY_SIZE_BYTES == 1_048_576

    def test_max_raw_payload_is_6mb(self):
        assert MAX_RAW_PAYLOAD_BYTES == 6_291_456

    def test_secret_cache_ttl_is_300(self):
        assert SECRET_CACHE_TTL == 300

    def test_secret_timeout_is_5(self):
        assert SECRET_TIMEOUT == 5


class TestConfig:
    """Tests for the Config dataclass."""

    def test_all_fields_set_correctly(self):
        config = Config(
            environment="dev",
            firehose_stream_name="my-stream",
            secret_id="arn:aws:secretsmanager:us-east-1:123:secret:webhook",
            log_level="DEBUG",
            log_payload=True,
        )
        assert config.environment == "dev"
        assert config.firehose_stream_name == "my-stream"
        assert config.secret_id == "arn:aws:secretsmanager:us-east-1:123:secret:webhook"
        assert config.log_level == "DEBUG"
        assert config.log_payload is True

    def test_default_log_level_is_info(self):
        config = Config(
            environment="prod",
            firehose_stream_name="stream",
            secret_id="secret-id",
        )
        assert config.log_level == "INFO"

    def test_default_log_payload_is_false(self):
        config = Config(
            environment="prod",
            firehose_stream_name="stream",
            secret_id="secret-id",
        )
        assert config.log_payload is False

    def test_instance_constants_accessible(self):
        config = Config(
            environment="dev",
            firehose_stream_name="stream",
            secret_id="secret-id",
        )
        assert config.schema_version == 1
        assert config.max_body_size_bytes == 1_048_576
        assert config.max_raw_payload_bytes == 6_291_456
        assert config.secret_cache_ttl == 300
        assert config.secret_timeout == 5

    def test_frozen_dataclass_is_immutable(self):
        config = Config(
            environment="dev",
            firehose_stream_name="stream",
            secret_id="secret-id",
        )
        with pytest.raises(AttributeError):
            config.environment = "prod"


class TestParseLogPayload:
    """Tests for the _parse_log_payload helper."""

    def test_true_lowercase(self):
        assert _parse_log_payload("true") is True

    def test_true_uppercase(self):
        assert _parse_log_payload("TRUE") is True

    def test_true_mixed_case(self):
        assert _parse_log_payload("True") is True

    def test_true_with_whitespace(self):
        assert _parse_log_payload("  true  ") is True

    def test_false_lowercase(self):
        assert _parse_log_payload("false") is False

    def test_false_uppercase(self):
        assert _parse_log_payload("FALSE") is False

    def test_empty_string(self):
        assert _parse_log_payload("") is False

    def test_arbitrary_string_is_false(self):
        assert _parse_log_payload("yes") is False
        assert _parse_log_payload("1") is False
        assert _parse_log_payload("enabled") is False


class TestBuildConfig:
    """Tests for _build_config reading from environment variables."""

    def test_all_env_vars_set(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "staging")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "my-firehose")
        monkeypatch.setenv("SECRET_ID", "arn:aws:secretsmanager:us-east-1:123:secret:test")
        monkeypatch.setenv("LOG_LEVEL", "debug")
        monkeypatch.setenv("LOG_PAYLOAD", "true")

        config = _build_config()
        assert config.environment == "staging"
        assert config.firehose_stream_name == "my-firehose"
        assert config.secret_id == "arn:aws:secretsmanager:us-east-1:123:secret:test"
        assert config.log_level == "DEBUG"
        assert config.log_payload is True

    def test_missing_environment_raises(self, monkeypatch):
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "stream")
        monkeypatch.setenv("SECRET_ID", "secret")

        with pytest.raises(EnvironmentError, match="ENVIRONMENT"):
            _build_config()

    def test_missing_firehose_stream_name_raises(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.delenv("FIREHOSE_STREAM_NAME", raising=False)
        monkeypatch.setenv("SECRET_ID", "secret")

        with pytest.raises(EnvironmentError, match="FIREHOSE_STREAM_NAME"):
            _build_config()

    def test_missing_secret_id_raises(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "stream")
        monkeypatch.delenv("SECRET_ID", raising=False)

        with pytest.raises(EnvironmentError, match="SECRET_ID"):
            _build_config()

    def test_defaults_for_optional_vars(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "prod")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "prod-stream")
        monkeypatch.setenv("SECRET_ID", "prod-secret")
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        monkeypatch.delenv("LOG_PAYLOAD", raising=False)

        config = _build_config()
        assert config.log_level == "INFO"
        assert config.log_payload is False

    def test_log_level_uppercased(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "stream")
        monkeypatch.setenv("SECRET_ID", "secret")
        monkeypatch.setenv("LOG_LEVEL", "warning")

        config = _build_config()
        assert config.log_level == "WARNING"

    def test_empty_string_environment_raises(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "stream")
        monkeypatch.setenv("SECRET_ID", "secret")

        with pytest.raises(EnvironmentError, match="ENVIRONMENT"):
            _build_config()

    def test_empty_string_firehose_raises(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "")
        monkeypatch.setenv("SECRET_ID", "secret")

        with pytest.raises(EnvironmentError, match="FIREHOSE_STREAM_NAME"):
            _build_config()

    def test_empty_string_secret_id_raises(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "stream")
        monkeypatch.setenv("SECRET_ID", "")

        with pytest.raises(EnvironmentError, match="SECRET_ID"):
            _build_config()


class TestGetConfig:
    """Tests for the get_config singleton function."""

    def test_returns_config_instance(self, monkeypatch):
        get_config.cache_clear()
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "stream")
        monkeypatch.setenv("SECRET_ID", "secret")

        config = get_config()
        assert isinstance(config, Config)
        assert config.environment == "dev"

    def test_caches_result(self, monkeypatch):
        get_config.cache_clear()
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "stream")
        monkeypatch.setenv("SECRET_ID", "secret")

        config1 = get_config()
        config2 = get_config()
        assert config1 is config2

    def test_cache_clear_allows_rebuild(self, monkeypatch):
        get_config.cache_clear()
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("FIREHOSE_STREAM_NAME", "stream")
        monkeypatch.setenv("SECRET_ID", "secret")

        config1 = get_config()
        get_config.cache_clear()

        monkeypatch.setenv("ENVIRONMENT", "prod")
        config2 = get_config()

        assert config1.environment == "dev"
        assert config2.environment == "prod"
        assert config1 is not config2

    def teardown_method(self):
        """Clear the cache after each test to prevent cross-test pollution."""
        get_config.cache_clear()
