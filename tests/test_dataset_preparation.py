
import csv

import numpy as np
import pytest

from backend.dataset_preparation import (
    NUMERIC_FEATURES,
    load_hdfs_dataset,
    split_hdfs_dataset,
)


def create_test_files(tmp_path):
    feature_file = tmp_path / "features.csv"
    label_file = tmp_path / "labels.csv"

    with feature_file.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "block_id",
                "log_count",
                "info_count",
                "warn_count",
                "first_seen",
                "last_seen",
                *[f"E{i}" for i in range(1, 30)],
            ],
        )
        writer.writeheader()

        for i in range(100):
            row = {
                "block_id": f"blk_{i}",
                "log_count": 10,
                "info_count": 8,
                "warn_count": 2,
                "first_seen": "2008-11-09 20:35:18",
                "last_seen": "2008-11-09 20:36:18",
            }

            for event_id in NUMERIC_FEATURES[3:]:
                row[event_id] = 1 if event_id == "E5" else 0

            writer.writerow(row)

    with label_file.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["BlockId", "Label"],
        )
        writer.writeheader()

        for i in range(100):
            writer.writerow({
                "BlockId": f"blk_{i}",
                "Label": "Anomaly" if i < 10 else "Normal",
            })

    return feature_file, label_file


def test_load_dataset_returns_correct_shapes(tmp_path):
    feature_file, label_file = create_test_files(tmp_path)

    X, y, block_ids, feature_names = load_hdfs_dataset(
        feature_file, label_file
    )

    assert X.shape == (100, 32)
    assert y.shape == (100,)
    assert len(block_ids) == 100
    assert len(feature_names) == 32


def test_labels_are_encoded_correctly(tmp_path):
    feature_file, label_file = create_test_files(tmp_path)

    X, y, block_ids, _ = load_hdfs_dataset(
        feature_file, label_file
    )

    assert np.sum(y == 1) == 10
    assert np.sum(y == 0) == 90


def test_identifiers_and_timestamps_are_not_features(tmp_path):
    feature_file, label_file = create_test_files(tmp_path)

    X, y, block_ids, feature_names = load_hdfs_dataset(
        feature_file, label_file
    )

    assert "block_id" not in feature_names
    assert "first_seen" not in feature_names
    assert "last_seen" not in feature_names
    assert X.shape[1] == 32


def test_stratified_split_preserves_anomaly_ratio(tmp_path):
    feature_file, label_file = create_test_files(tmp_path)

    X, y, block_ids, _ = load_hdfs_dataset(
        feature_file, label_file
    )

    X_train, X_test, y_train, y_test, ids_train, ids_test = (
        split_hdfs_dataset(X, y, block_ids)
    )

    assert len(X_train) == 80
    assert len(X_test) == 20

    assert np.sum(y_train == 1) == 8
    assert np.sum(y_test == 1) == 2

    assert set(ids_train).isdisjoint(set(ids_test))


def test_missing_ground_truth_label_is_rejected(tmp_path):
    feature_file, label_file = create_test_files(tmp_path)

    rows = list(csv.DictReader(feature_file.open(encoding="utf-8")))

    with feature_file.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file, fieldnames=rows[0].keys()
        )
        writer.writeheader()
        writer.writerows(rows[:99])

    with pytest.raises(ValueError, match="no features"):
        load_hdfs_dataset(feature_file, label_file)


def test_duplicate_feature_block_is_rejected(tmp_path):
    feature_file, label_file = create_test_files(tmp_path)

    rows = list(csv.DictReader(feature_file.open(encoding="utf-8")))

    with feature_file.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file, fieldnames=rows[0].keys()
        )
        writer.writeheader()
        writer.writerows(rows + [rows[0]])

    with pytest.raises(ValueError, match="Duplicate feature block"):
        load_hdfs_dataset(feature_file, label_file)
