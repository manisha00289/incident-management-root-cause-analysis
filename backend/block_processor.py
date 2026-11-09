
import csv
from array import array
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


# All 29 event IDs from the official HDFS template catalog.
EVENT_IDS = tuple(f"E{i}" for i in range(1, 30))

EVENT_INDEX = {
    event_id: index
    for index, event_id in enumerate(EVENT_IDS)
}


@dataclass
class BlockFeatures:
    """Aggregated features for one HDFS block."""

    event_counts: array = field(
        default_factory=lambda: array("I", [0]) * len(EVENT_IDS)
    )

    log_count: int = 0
    info_count: int = 0
    warn_count: int = 0

    first_seen: datetime | None = None
    last_seen: datetime | None = None


class BlockFeatureAggregator:
    """
    Aggregate parsed HDFS records by block ID.

    Ground-truth anomaly labels are deliberately not loaded here.
    """

    def __init__(self):
        self._blocks: dict[str, BlockFeatures] = {}

    @property
    def block_count(self) -> int:
        return len(self._blocks)

    def add_record(self, record: dict, event_id: str) -> None:
        """Add one parsed log record to its associated block(s)."""

        if event_id not in EVENT_INDEX:
            raise ValueError(f"Unknown event ID: {event_id}")

        block_ids = record.get("block_ids", [])

        # Remove duplicate IDs while preserving their order.
        block_ids = list(dict.fromkeys(block_ids))

        if not block_ids:
            raise ValueError("Record does not contain a block ID.")

        timestamp = record.get("timestamp")

        if not isinstance(timestamp, datetime):
            raise ValueError("Record must contain a valid datetime.")

        level = record.get("level")
        event_index = EVENT_INDEX[event_id]

        for block_id in block_ids:
            if not isinstance(block_id, str) or not block_id:
                raise ValueError("Block IDs must be non-empty strings.")

            features = self._blocks.setdefault(
                block_id,
                BlockFeatures()
            )

            features.log_count += 1
            features.event_counts[event_index] += 1

            if level == "INFO":
                features.info_count += 1
            elif level == "WARN":
                features.warn_count += 1

            if (
                features.first_seen is None
                or timestamp < features.first_seen
            ):
                features.first_seen = timestamp

            if (
                features.last_seen is None
                or timestamp > features.last_seen
            ):
                features.last_seen = timestamp

    def get_block_features(self, block_id: str) -> dict | None:
        """Return aggregated features for one block."""

        features = self._blocks.get(block_id)

        if features is None:
            return None

        return {
            "block_id": block_id,
            "log_count": features.log_count,
            "info_count": features.info_count,
            "warn_count": features.warn_count,
            "first_seen": features.first_seen,
            "last_seen": features.last_seen,
            "event_counts": {
                event_id: features.event_counts[index]
                for index, event_id in enumerate(EVENT_IDS)
            },
        }

    def write_csv(self, output_path: str | Path) -> None:
        """Write the aggregated block features to a CSV file."""

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "block_id",
            "log_count",
            "info_count",
            "warn_count",
            "first_seen",
            "last_seen",
            *EVENT_IDS,
        ]

        with output_path.open(
            "w",
            newline="",
            encoding="utf-8"
        ) as file:

            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames
            )

            writer.writeheader()

            for block_id in sorted(self._blocks):
                features = self._blocks[block_id]

                row = {
                    "block_id": block_id,
                    "log_count": features.log_count,
                    "info_count": features.info_count,
                    "warn_count": features.warn_count,
                    "first_seen": features.first_seen.isoformat(
                        sep=" "
                    ),
                    "last_seen": features.last_seen.isoformat(
                        sep=" "
                    ),
                }

                for index, event_id in enumerate(EVENT_IDS):
                    row[event_id] = features.event_counts[index]

                writer.writerow(row)
