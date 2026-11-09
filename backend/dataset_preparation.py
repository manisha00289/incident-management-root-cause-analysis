
import csv
from pathlib import Path

import numpy as np
from sklearn.model_selection import train_test_split


NUMERIC_FEATURES = [
    "log_count",
    "info_count",
    "warn_count",
    *[f"E{i}" for i in range(1, 30)],
]

LABEL_MAPPING = {
    "Normal": 0,
    "Anomaly": 1,
}


def load_hdfs_dataset(
    feature_file: str | Path,
    label_file: str | Path,
):
    """
    Load block-level features and join official HDFS labels.

    Ground-truth labels are kept separate from model features.
    """

    feature_file = Path(feature_file)
    label_file = Path(label_file)

    labels = {}

    with label_file.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as file:
        for row in csv.DictReader(file):
            block_id = row["BlockId"]
            label = row["Label"]

            if block_id in labels:
                raise ValueError(
                    f"Duplicate ground-truth block: {block_id}"
                )

            if label not in LABEL_MAPPING:
                raise ValueError(
                    f"Unexpected label: {label}"
                )

            labels[block_id] = LABEL_MAPPING[label]

    features = []
    targets = []
    block_ids = []

    seen_blocks = set()

    with feature_file.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as file:

        reader = csv.DictReader(file)

        missing_columns = (
            set(NUMERIC_FEATURES) - set(reader.fieldnames or [])
        )

        if missing_columns:
            raise ValueError(
                f"Missing feature columns: {sorted(missing_columns)}"
            )

        for row in reader:
            block_id = row["block_id"]

            if block_id in seen_blocks:
                raise ValueError(
                    f"Duplicate feature block: {block_id}"
                )

            if block_id not in labels:
                raise ValueError(
                    f"Missing label for block: {block_id}"
                )

            try:
                values = [
                    float(row[name])
                    for name in NUMERIC_FEATURES
                ]
            except (ValueError, TypeError) as error:
                raise ValueError(
                    f"Invalid feature value for {block_id}"
                ) from error

            if not np.isfinite(values).all():
                raise ValueError(
                    f"Non-finite feature value for {block_id}"
                )

            features.append(values)
            targets.append(labels[block_id])
            block_ids.append(block_id)

            seen_blocks.add(block_id)

    if seen_blocks != set(labels):
        missing_features = set(labels) - seen_blocks

        raise ValueError(
            f"{len(missing_features)} labeled blocks have no features."
        )

    X = np.asarray(features, dtype=np.float32)
    y = np.asarray(targets, dtype=np.int8)

    return X, y, block_ids, NUMERIC_FEATURES


def split_hdfs_dataset(
    X,
    y,
    block_ids,
    test_size=0.20,
    random_state=42,
):
    """Create a reproducible, stratified training/test split."""

    return train_test_split(
        X,
        y,
        block_ids,
        test_size=test_size,
        random_state=random_state,
        stratify=y,
    )
