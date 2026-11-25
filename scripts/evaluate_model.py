"""
Reproducible evaluation of the Random Forest HDFS anomaly detector.

Run from the project root (after building block features):
    python scripts/evaluate_model.py \
        --features data/processed/block_features.csv \
        --labels data/raw/anomaly_label.csv \
        --output reports/model_evaluation.json

The script runs two evaluations and saves every metric to one JSON report:

1. Random split: a stratified 80/20 train/test split. It also measures
   how many test rows share an exact feature pattern with a training row,
   which shows whether the split mostly tests on patterns already seen.

2. Pattern-separated validation: StratifiedGroupKFold, grouping rows by
   their exact feature pattern, so no pattern appears in both the
   training and validation folds. This is a stricter test of how the
   model generalizes to unseen patterns.
"""

import argparse
import json
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import sklearn
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedGroupKFold

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from backend.anomaly_detector import train_random_forest  # noqa: E402
from backend.dataset_preparation import (  # noqa: E402
    load_hdfs_dataset,
    split_hdfs_dataset,
)

RANDOM_STATE = 42


def pattern_groups(X):
    """Give every distinct feature row (pattern) its own integer group ID."""
    _, group_ids = np.unique(X, axis=0, return_inverse=True)
    return group_ids.ravel()


def pattern_overlap(X_train, X_test):
    """Share of test rows whose exact feature pattern also appears in training."""
    train_patterns = {row.tobytes() for row in X_train}
    seen = sum(row.tobytes() in train_patterns for row in X_test)
    return seen / len(X_test) if len(X_test) else 0.0


def score(y_true, y_pred, y_prob):
    """Anomaly-class metrics. Threshold-free metrics need both classes present."""
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    both_classes = len(np.unique(y_true)) == 2

    return {
        "rows": int(len(y_true)),
        "anomalies": int(np.sum(y_true)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, y_prob)) if both_classes else None,
        "average_precision": (
            float(average_precision_score(y_true, y_prob)) if both_classes else None
        ),
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
    }


def evaluate_random_split(X, y, block_ids):
    X_train, X_test, y_train, y_test, _, _ = split_hdfs_dataset(
        X, y, block_ids, test_size=0.20, random_state=RANDOM_STATE
    )

    model = train_random_forest(X_train, y_train)
    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = (y_prob >= 0.5).astype(int)

    result = score(y_test, y_pred, y_prob)
    result["test_pattern_overlap_with_train"] = pattern_overlap(X_train, X_test)
    return result


def evaluate_grouped_folds(X, y, n_splits=5):
    groups = pattern_groups(X)
    splitter = StratifiedGroupKFold(
        n_splits=n_splits, shuffle=True, random_state=RANDOM_STATE
    )

    folds = []
    for fold, (train_idx, test_idx) in enumerate(splitter.split(X, y, groups), start=1):
        model = train_random_forest(X[train_idx], y[train_idx])
        y_prob = model.predict_proba(X[test_idx])[:, 1]
        y_pred = (y_prob >= 0.5).astype(int)

        result = score(y[test_idx], y_pred, y_prob)
        result["fold"] = fold
        result["test_pattern_overlap_with_train"] = pattern_overlap(
            X[train_idx], X[test_idx]
        )
        folds.append(result)
        print(
            f"  Fold {fold}: precision={result['precision']:.4f} "
            f"recall={result['recall']:.4f} rows={result['rows']:,}"
        )

    def summarize(metric):
        values = [f[metric] for f in folds if f[metric] is not None]
        if not values:
            return None
        return {
            "mean": float(np.mean(values)),
            "min": float(np.min(values)),
            "max": float(np.max(values)),
        }

    return {
        "n_splits": n_splits,
        "unique_patterns": int(len(np.unique(groups))),
        "folds": folds,
        "summary": {
            metric: summarize(metric)
            for metric in ("precision", "recall", "f1", "roc_auc", "average_precision")
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Evaluate the HDFS anomaly detector.")
    parser.add_argument("--features", default="data/processed/block_features.csv")
    parser.add_argument("--labels", default="data/raw/anomaly_label.csv")
    parser.add_argument("--output", default="reports/model_evaluation.json")
    parser.add_argument("--folds", type=int, default=5)
    args = parser.parse_args()

    start = time.time()

    print("Loading dataset...")
    X, y, block_ids, feature_names = load_hdfs_dataset(args.features, args.labels)
    print(f"  {len(y):,} blocks, {int(y.sum()):,} anomalies, {X.shape[1]} features")

    print("\n1. Random stratified 80/20 split")
    random_split = evaluate_random_split(X, y, block_ids)
    print(
        f"  precision={random_split['precision']:.4f} "
        f"recall={random_split['recall']:.4f} "
        f"pattern overlap={random_split['test_pattern_overlap_with_train']:.2%}"
    )

    print(f"\n2. Pattern-separated StratifiedGroupKFold ({args.folds} folds)")
    grouped = evaluate_grouped_folds(X, y, n_splits=args.folds)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "environment": {
            "python": platform.python_version(),
            "scikit_learn": sklearn.__version__,
            "numpy": np.__version__,
            "random_state": RANDOM_STATE,
        },
        "dataset": {
            "blocks": int(len(y)),
            "anomalies": int(y.sum()),
            "features": feature_names,
        },
        "model": "RandomForestClassifier(n_estimators=200, class_weight='balanced')",
        "random_split": random_split,
        "pattern_separated_validation": grouped,
        "runtime_seconds": round(time.time() - start, 1),
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nReport written to {output}")


if __name__ == "__main__":
    main()
