"""
Generate a sample incident report from the bundled example log.

Run from the project root:
    python scripts/generate_sample_report.py

Reads:  examples/sample_application_log.txt
Writes: examples/sample_incident_report.json

This script does not write to the SQLite database.
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from backend.main import (  # noqa: E402
    detect_incidents,
    parse_log_line,
    summarize_incidents,
)

INPUT_FILE = PROJECT_ROOT / "examples" / "sample_application_log.txt"
OUTPUT_FILE = PROJECT_ROOT / "examples" / "sample_incident_report.json"


def main():
    records = []
    skipped = 0

    with INPUT_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            record = parse_log_line(line)
            if record is None:
                skipped += 1
                continue

            records.append(record)

    incidents = detect_incidents(records)

    report = {
        "source_file": INPUT_FILE.name,
        "lines_parsed": len(records),
        "lines_skipped": skipped,
        "summary": summarize_incidents(incidents),
        "incidents": incidents,
    }

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)

    print(f"Parsed {len(records)} lines, skipped {skipped}.")
    print(f"Detected {len(incidents)} incident(s).")
    print(f"Report written to {OUTPUT_FILE.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
