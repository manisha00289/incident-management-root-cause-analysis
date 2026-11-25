import csv
import importlib.util
import json
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_script(name):
    path = PROJECT_ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


evaluate_model = load_script("evaluate_model")
build_hdfs_features = load_script("build_hdfs_features")


def make_dataset(rows=200, seed=0):
    rng = np.random.default_rng(seed)
    X = rng.integers(0, 5, size=(rows, 32)).astype(np.float32)
    y = np.zeros(rows, dtype=np.int8)
    y[: rows // 5] = 1
    X[y == 1, 3] += 10  # make anomalies separable on one feature
    return X, y


def test_pattern_groups_give_identical_rows_the_same_group():
    X = np.array([[1, 2], [3, 4], [1, 2]], dtype=np.float32)
    groups = evaluate_model.pattern_groups(X)
    assert groups[0] == groups[2]
    assert groups[0] != groups[1]


def test_pattern_overlap_counts_seen_patterns():
    X_train = np.array([[1, 2], [3, 4]], dtype=np.float32)
    X_test = np.array([[1, 2], [5, 6]], dtype=np.float32)
    assert evaluate_model.pattern_overlap(X_train, X_test) == 0.5


def test_grouped_folds_never_share_patterns():
    X, y = make_dataset()
    result = evaluate_model.evaluate_grouped_folds(X, y, n_splits=3)
    assert len(result["folds"]) == 3
    for fold in result["folds"]:
        assert fold["test_pattern_overlap_with_train"] == 0.0
    assert result["summary"]["precision"]["mean"] > 0.9


def test_build_features_writes_block_rows(tmp_path):
    templates = tmp_path / "templates.csv"
    with templates.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["EventId", "EventTemplate"])
        writer.writerow(["E1", "Receiving block [*] src: [*] dest: [*]"])
        writer.writerow(["E2", "PacketResponder [*] for block [*] terminating"])

    log = tmp_path / "HDFS.log"
    log.write_text(
        "081109 203615 148 INFO dfs.DataNode$DataXceiver: "
        "Receiving block blk_1 src: /10.0.0.1:1 dest: /10.0.0.2:2\n"
        "081109 203616 149 INFO dfs.DataNode$PacketResponder: "
        "PacketResponder 1 for block blk_1 terminating\n"
        "081109 203617 150 WARN dfs.DataNode$PacketResponder: "
        "PacketResponder 2 for block blk_2 terminating\n"
        "not an hdfs line\n",
        encoding="utf-8",
    )

    output = tmp_path / "features.csv"
    stats = build_hdfs_features.build_features(log, templates, output, progress_every=0)

    assert stats["blocks_written"] == 2
    assert stats["unparsed_lines"] == 1

    rows = {row["block_id"]: row for row in csv.DictReader(output.open())}
    assert rows["blk_1"]["E1"] == "1" and rows["blk_1"]["E2"] == "1"
    assert rows["blk_2"]["warn_count"] == "1"


def test_sample_report_matches_example_log():
    report = json.loads(
        (PROJECT_ROOT / "examples" / "sample_incident_report.json").read_text()
    )
    assert report["summary"]["total_incidents"] == len(report["incidents"])
