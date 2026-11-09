
from datetime import datetime

from backend.hdfs_parser import parse_hdfs_line


def test_parse_valid_hdfs_log_line():
    line = (
        "081109 203518 143 INFO "
        "dfs.DataNode$DataXceiver: "
        "Receiving block blk_-1608999687919862906 "
        "src: /10.250.19.102:54106 "
        "dest: /10.250.19.102:50010"
    )

    result = parse_hdfs_line(line)

    assert result is not None
    assert result["timestamp"] == datetime(2008, 11, 9, 20, 35, 18)
    assert result["process_id"] == 143
    assert result["level"] == "INFO"
    assert result["component"] == "dfs.DataNode$DataXceiver"
    assert result["block_ids"] == ["blk_-1608999687919862906"]


def test_duplicate_block_ids_are_removed():
    line = (
        "081109 204524 19 INFO dfs.FSDataset: "
        "Deleting block blk_-8213344449220111733 "
        "file /mnt/hadoop/blk_-8213344449220111733"
    )

    result = parse_hdfs_line(line)

    assert result is not None
    assert result["block_ids"] == ["blk_-8213344449220111733"]


def test_distinct_block_ids_are_preserved():
    line = (
        "081109 203518 143 INFO dfs.DataNode: "
        "Processing blk_123 and blk_456"
    )

    result = parse_hdfs_line(line)

    assert result is not None
    assert result["block_ids"] == ["blk_123", "blk_456"]


def test_invalid_log_format_returns_none():
    line = "This is not a valid HDFS log record"

    result = parse_hdfs_line(line)

    assert result is None


def test_valid_log_without_block_id_returns_empty_list():
    line = (
        "081109 203518 143 INFO dfs.DataNode: "
        "Pipeline started"
    )

    result = parse_hdfs_line(line)

    assert result is not None
    assert result["block_ids"] == []
