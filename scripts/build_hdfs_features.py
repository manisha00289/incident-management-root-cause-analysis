"""
Build block-level features from the raw HDFS benchmark log.

Run from the project root:
    python scripts/build_hdfs_features.py \
        --log-file data/raw/HDFS.log \
        --templates data/raw/HDFS.log_templates.csv \
        --output data/processed/block_features.csv

Download HDFS.log and HDFS.log_templates.csv from LogHub first
(https://github.com/logpai/loghub). The full log has about 11 million
lines, so expect this step to take a while.
"""

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from backend.block_processor import BlockFeatureAggregator  # noqa: E402
from backend.event_matcher import (  # noqa: E402
    load_event_templates,
    match_event_template,
)
from backend.hdfs_parser import parse_hdfs_line  # noqa: E402


def build_features(log_file, template_file, output_file, progress_every=1_000_000):
    templates = load_event_templates(template_file)
    aggregator = BlockFeatureAggregator()

    stats = {
        "lines_read": 0,
        "unparsed_lines": 0,
        "unmatched_events": 0,
        "lines_without_block": 0,
        "records_aggregated": 0,
    }

    start = time.time()

    with Path(log_file).open("r", encoding="utf-8", errors="replace") as file:
        for line in file:
            stats["lines_read"] += 1

            record = parse_hdfs_line(line)
            if record is None:
                stats["unparsed_lines"] += 1
                continue

            event_id = match_event_template(record["message"], templates)
            if event_id is None:
                stats["unmatched_events"] += 1
                continue

            if not record["block_ids"]:
                stats["lines_without_block"] += 1
                continue

            aggregator.add_record(record, event_id)
            stats["records_aggregated"] += 1

            if progress_every and stats["lines_read"] % progress_every == 0:
                elapsed = time.time() - start
                print(f"  {stats['lines_read']:,} lines processed ({elapsed:,.0f}s)")

    aggregator.write_csv(output_file)
    stats["blocks_written"] = aggregator.block_count

    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--log-file", default="data/raw/HDFS.log")
    parser.add_argument("--templates", default="data/raw/HDFS.log_templates.csv")
    parser.add_argument("--output", default="data/processed/block_features.csv")
    args = parser.parse_args()

    print("Building HDFS block features...")
    stats = build_features(args.log_file, args.templates, args.output)

    print("\nFeature build summary")
    for name, value in stats.items():
        print(f"  {name.replace('_', ' ').title()}: {value:,}")
    print(f"\nFeatures written to {args.output}")


if __name__ == "__main__":
    main()
