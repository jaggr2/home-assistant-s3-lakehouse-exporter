"""Key layout partitioning."""

from __future__ import annotations

import re
from datetime import datetime

from exporter.writers.base import build_object_key

from tests.conftest import make_options, make_rows, ts


def test_key_layout() -> None:
    key = build_object_key("home-assistant", "ha_state_changes", ts(2026, 10, 24, 7, 5), ".parquet")
    assert key.startswith("home-assistant/ha_state_changes/year=2026/month=10/day=24/part-")
    assert key.endswith(".parquet")
    assert re.search(r"/part-[0-9a-f]{32}\.parquet$", key)


def test_empty_prefix_has_no_leading_segment() -> None:
    key = build_object_key("", "ha_events", ts(2026, 1, 2, 3), ".jsonl.zst")
    assert key.startswith("ha_events/year=2026/month=01/day=02/part-")


def test_key_parts_are_zero_padded() -> None:
    key = build_object_key("p", "t", ts(2026, 3, 4, 0), ".parquet")
    assert "/month=03/day=04/" in key


def test_partitions_use_utc_date() -> None:
    # 23:30 at UTC-2 is already the next day in UTC
    local = datetime.fromisoformat("2026-01-01T23:30:00-02:00")
    key = build_object_key("p", "t", local, ".parquet")
    assert "/year=2026/month=01/day=02/" in key


def test_unique_keys_per_call() -> None:
    keys = {build_object_key("p", "t", ts(2026, 1, 2, 3), ".parquet") for _ in range(20)}
    assert len(keys) == 20


def test_partition_rows_groups_by_kind_and_day() -> None:
    rows = make_rows(
        [
            ("state_changes", ts(2026, 10, 24, 23, 59), {"a": 1}),
            ("state_changes", ts(2026, 10, 25, 0, 1), {"a": 2}),
            ("events", ts(2026, 10, 24, 10), {"a": 3}),
        ]
    )
    options = make_options()
    from exporter.writers.jsonl import JsonlWriter

    from tests.conftest import FakeStore

    writer = JsonlWriter(FakeStore(), options)
    groups = writer.partition_rows(rows)
    assert set(groups) == {
        ("state_changes", ts(2026, 10, 24).date()),
        ("state_changes", ts(2026, 10, 25).date()),
        ("events", ts(2026, 10, 24).date()),
    }
    assert [row.payload["a"] for row in groups[("state_changes", ts(2026, 10, 24).date())]] == [1]
