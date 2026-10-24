"""Option parsing, validation and normalisation."""

from __future__ import annotations

import json

import pytest
from exporter.config import ConfigError, load_options, parse_options

from tests.conftest import make_options


def test_defaults_applied() -> None:
    options = make_options()
    assert options.s3.bucket == "test-bucket"
    assert options.s3.storage_class == "STANDARD"
    assert options.s3.endpoint_url is None
    assert options.s3.path_style is False
    assert options.capture.attributes == "all"
    assert options.capture.events == ("state_changed",)
    assert options.capture.registries is True
    assert options.capture.backfill_days == 30
    assert options.capture.flush_interval_seconds == 300
    assert options.capture.max_batch_rows == 20000
    assert options.format.table_prefix == "ha_"
    assert options.format.compression == "zstd"
    assert options.runtime.spool_max_mb == 256
    assert options.runtime.spool_max_bytes == 256 * 1024 * 1024
    assert options.runtime.log_level == "info"
    assert options.runtime.notify_webhook is None


def test_optional_strings_normalize_to_none() -> None:
    options = make_options(
        auth={"access_key_id": " ", "secret_access_key": None, "session_token": ""},
        s3={"endpoint_url": ""},
    )
    assert options.auth.access_key_id is None
    assert options.auth.secret_access_key is None
    assert options.auth.session_token is None
    assert options.s3.endpoint_url is None


def test_prefix_is_normalized() -> None:
    options = make_options(s3={"prefix": " /home-assistant/ "})
    assert options.s3.prefix == "home-assistant"


def test_table_name_uses_prefix() -> None:
    options = make_options(format={"table_prefix": "x_"})
    assert options.table_name("state_changes") == "x_state_changes"


def test_spool_is_clamped_to_hard_cap() -> None:
    options = make_options(runtime={"spool_max_mb": 4096})
    assert options.runtime.spool_max_mb == 512


def test_spool_below_one_rejected() -> None:
    with pytest.raises(ConfigError):
        make_options(runtime={"spool_max_mb": 0})


def test_flush_interval_clamped_to_minimum() -> None:
    options = make_options(capture={"flush_interval_seconds": 1})
    assert options.capture.flush_interval_seconds == 10


def test_allowlist_mode_requires_allowlist() -> None:
    with pytest.raises(ConfigError):
        make_options(capture={"attributes": "allowlist"})
    options = make_options(
        capture={"attributes": "allowlist", "attribute_allowlist": ["unit_of_measurement"]}
    )
    assert options.capture.attribute_allowlist == ("unit_of_measurement",)


def test_minimal_mode_allows_empty_allowlist() -> None:
    options = make_options(capture={"attributes": "minimal"})
    assert options.capture.attributes == "minimal"
    assert options.capture.attribute_allowlist == ()


def test_events_are_deduped_and_include_state_changed() -> None:
    options = make_options(
        capture={"events": ["automation_triggered", "state_changed", "automation_triggered"]}
    )
    # de-duplicated, and state_changed is always captured
    assert options.capture.events == ("automation_triggered", "state_changed")
    assert make_options(capture={"events": ["call_service"]}).capture.events == (
        "state_changed",
        "call_service",
    )


def test_invalid_event_type_rejected() -> None:
    with pytest.raises(ConfigError):
        make_options(capture={"events": ["Not An Event"]})


def test_iceberg_requires_catalog_uri_for_non_glue() -> None:
    with pytest.raises(ConfigError):
        make_options(format={"type": "iceberg", "catalog": "sql"})
    options = make_options(
        format={"type": "iceberg", "catalog": "glue"}
    )
    assert options.format.catalog == "glue"
    options = make_options(
        format={
            "type": "iceberg",
            "catalog": "sql",
            "catalog_uri": "sqlite:///catalog.db",
            "warehouse": "s3://test-bucket/wh",
        }
    )
    assert options.format.catalog_uri == "sqlite:///catalog.db"


def test_non_iceberg_drops_catalog() -> None:
    options = make_options(format={"type": "jsonl", "compression": "zstd"})
    assert options.format.catalog is None


def test_jsonl_rejects_snappy() -> None:
    with pytest.raises(ConfigError):
        make_options(format={"type": "jsonl", "compression": "snappy"})


def test_invalid_table_prefix_rejected() -> None:
    with pytest.raises(ConfigError):
        make_options(format={"table_prefix": "bad-prefix"})


def test_secret_pairing_rules() -> None:
    with pytest.raises(ConfigError):
        make_options(auth={"access_key_id": None, "secret_access_key": "only-secret"})
    with pytest.raises(ConfigError):
        make_options(
            auth={
                "access_key_id": "id",
                "secret_access_key": None,
                "session_token": "token",
            }
        )


def test_webhook_must_be_http() -> None:
    with pytest.raises(ConfigError):
        make_options(runtime={"notify_webhook": "ftp://example.invalid/hook"})
    options = make_options(runtime={"notify_webhook": "https://example.invalid/hook"})
    assert options.runtime.notify_webhook == "https://example.invalid/hook"


def test_missing_section_is_tolerated() -> None:
    options = parse_options({"s3": {"bucket": "b"}})
    assert options.capture.attributes == "all"
    assert options.runtime.spool_max_mb == 256


def test_bad_types_rejected() -> None:
    with pytest.raises(ConfigError):
        parse_options({"s3": ["not", "a", "mapping"]})
    with pytest.raises(ConfigError):
        make_options(capture={"backfill_days": "30"})
    with pytest.raises(ConfigError):
        make_options(s3={"path_style": "yes"})


def test_negative_backfill_rejected() -> None:
    with pytest.raises(ConfigError):
        make_options(capture={"backfill_days": -1})
    assert make_options(capture={"backfill_days": 0}).capture.backfill_days == 0


def test_load_options_from_file(tmp_path) -> None:
    path = tmp_path / "options.json"
    path.write_text(json.dumps({"s3": {"bucket": "b"}}), encoding="utf-8")
    options = load_options(path)
    assert options.s3.bucket == "b"


def test_load_options_missing_file(tmp_path) -> None:
    with pytest.raises(ConfigError):
        load_options(tmp_path / "missing.json")
