
import csv
from datetime import datetime

import pytest

from backend.block_processor import (
    BlockFeatureAggregator,
    EVENT_IDS,
)


def make_record(block_ids, timestamp, level="INFO"):
    return {
        "timestamp": datetime.fromisoformat(timestamp),
        "process_id": 143,
        "level": level,
        "component": "dfs.DataNode",
        "message": "Test HDFS event",
        "block_ids": block_ids,
    }


def test_single_record_creates_block_features():
    aggregator = BlockFeatureAggregator()

    record = make_record(
        ["blk_123"],
        "2008-11-09 20:35:18",
        "INFO",
    )

    aggregator.add_record(record, "E5")

    result = aggregator.get_block_features("blk_123")

    assert result is not None
    assert result["log_count"] == 1
    assert result["info_count"] == 1
    assert result["warn_count"] == 0
    assert result["event_counts"]["E5"] == 1


def test_duplicate_block_ids_are_not_double_counted():
    aggregator = BlockFeatureAggregator()

    record = make_record(
        ["blk_123", "blk_123"],
        "2008-11-09 20:35:18",
    )

    aggregator.add_record(record, "E21")

    result = aggregator.get_block_features("blk_123")

    assert result["log_count"] == 1
    assert result["event_counts"]["E21"] == 1


def test_distinct_block_ids_receive_the_record():
    aggregator = BlockFeatureAggregator()

    record = make_record(
        ["blk_123", "blk_456"],
        "2008-11-09 20:35:18",
    )

    aggregator.add_record(record, "E5")

    assert aggregator.block_count == 2
    assert aggregator.get_block_features("blk_123")["log_count"] == 1
    assert aggregator.get_block_features("blk_456")["log_count"] == 1


def test_first_and_last_timestamps_are_correct():
    aggregator = BlockFeatureAggregator()

    aggregator.add_record(
        make_record(["blk_123"], "2008-11-09 20:40:00"),
        "E5",
    )

    aggregator.add_record(
        make_record(["blk_123"], "2008-11-09 20:35:00", "WARN"),
        "E17",
    )

    result = aggregator.get_block_features("blk_123")

    assert result["first_seen"] == datetime(2008, 11, 9, 20, 35)
    assert result["last_seen"] == datetime(2008, 11, 9, 20, 40)
    assert result["warn_count"] == 1


def test_record_without_block_id_is_rejected():
    aggregator = BlockFeatureAggregator()

    record = make_record(
        [],
        "2008-11-09 20:35:18",
    )

    with pytest.raises(ValueError, match="block ID"):
        aggregator.add_record(record, "E5")


def test_unknown_event_id_is_rejected():
    aggregator = BlockFeatureAggregator()

    record = make_record(
        ["blk_123"],
        "2008-11-09 20:35:18",
    )

    with pytest.raises(ValueError, match="Unknown event ID"):
        aggregator.add_record(record, "E99")


def test_csv_export_contains_block_features(tmp_path):
    aggregator = BlockFeatureAggregator()

    record = make_record(
        ["blk_123"],
        "2008-11-09 20:35:18",
    )

    aggregator.add_record(record, "E5")

    output_file = tmp_path / "block_features.csv"
    aggregator.write_csv(output_file)

    with output_file.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == 1
    assert rows[0]["block_id"] == "blk_123"
    assert rows[0]["log_count"] == "1"
    assert rows[0]["E5"] == "1"
    assert rows[0]["E1"] == "0"
    assert set(EVENT_IDS).issubset(rows[0].keys())
