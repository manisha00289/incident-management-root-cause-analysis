
import re
from datetime import datetime


# Matches the structure of an HDFS log record.
LOG_PATTERN = re.compile(
    r"^(?P<date>\d{6})\s+"
    r"(?P<time>\d{6})\s+"
    r"(?P<process_id>\d+)\s+"
    r"(?P<level>[A-Z]+)\s+"
    r"(?P<component>[^:]+):\s*"
    r"(?P<message>.*)$"
)

# Finds HDFS block identifiers.
BLOCK_PATTERN = re.compile(r"\bblk_-?\d+\b")


def parse_hdfs_line(line: str) -> dict | None:
    """
    Parse one HDFS log line into structured fields.

    Returns None when the line does not match
    the expected HDFS log format.
    """

    match = LOG_PATTERN.match(line.strip())

    if not match:
        return None

    data = match.groupdict()

    # Convert the raw date and time into a datetime object.
    timestamp = datetime.strptime(
        f"{data['date']} {data['time']}",
        "%y%m%d %H%M%S"
    )

    # Extract unique block IDs while preserving their order.
    block_ids = list(
        dict.fromkeys(BLOCK_PATTERN.findall(data["message"]))
    )

    return {
        "timestamp": timestamp,
        "process_id": int(data["process_id"]),
        "level": data["level"],
        "component": data["component"].strip(),
        "message": data["message"],
        "block_ids": block_ids,
    }
