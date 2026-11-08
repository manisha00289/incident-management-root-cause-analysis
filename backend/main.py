import json
from pathlib import Path
import re
from backend.database import save_incidents as save_incidents_to_db
LOG_FILE = Path("data/application.log")

LOG_PATTERN = re.compile(
    r'^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) '
    r'(?P<severity>INFO|WARN|ERROR) '
    r'pipeline=(?P<pipeline>\S+)'
    r'(?: records=(?P<records>\d+))?'
    r'(?: error_code=(?P<error_code>\S+))? '
    r'message="(?P<message>.*)"$'
)


def parse_log_line(line):
    match = LOG_PATTERN.match(line)

    if match is None:
        return None

    record = match.groupdict()

    if record["records"] is not None:
        record["records"] = int(record["records"])

    return record

def determine_severity(error_code, error_count):
    if error_code == "DB_CONN_TIMEOUT" and error_count >= 2:
        return "HIGH"

    if error_count >= 2:
        return "MEDIUM"

    return "LOW"

def summarize_incidents(incidents):
    summary = {
        "total_incidents": len(incidents),
        "high_severity": 0,
        "medium_severity": 0,
        "low_severity": 0,
        "total_errors": 0,
        "affected_pipelines": 0,
    }

    pipelines = set()

    for incident in incidents:
        severity = incident["severity"]

        if severity == "HIGH":
            summary["high_severity"] += 1
        elif severity == "MEDIUM":
            summary["medium_severity"] += 1
        elif severity == "LOW":
            summary["low_severity"] += 1

        summary["total_errors"] += incident["error_count"]
        pipelines.add(incident["pipeline"])

    summary["affected_pipelines"] = len(pipelines)

    return summary
def detect_incidents(records):
    error_groups = {}

    # Group errors by pipeline name and error code.
    for record in records:
        error_code = record["error_code"]

        if record["severity"] != "ERROR" or not error_code:
            continue

        key = (record["pipeline"], error_code)

        if key not in error_groups:
            error_groups[key] = []

        error_groups[key].append(record)


    incidents = []

    # Create an incident for each group with at least two errors.
    for (pipeline, error_code), errors in error_groups.items():
        if len(errors) < 2:
            continue

        timestamps = [
            error["timestamp"]
            for error in errors
            if error.get("timestamp")
        ]

        incident = {
            "incident_id": f"INC-{len(incidents) + 1:04d}",
            "title": f"Repeated error: {error_code}",
            "severity": determine_severity(error_code, len(errors)),
            "pipeline": pipeline,
            "error_count": len(errors),
            "error_code": error_code,
            "status": "OPEN",
            "first_seen": min(timestamps) if timestamps else None,
            "last_seen": max(timestamps) if timestamps else None,
            "evidence": [
                {
                    "timestamp": error.get("timestamp"),
                    "message": error["message"],
                }
                for error in errors
            ],
        }

        incidents.append(incident)

    return incidents


def save_incidents(incidents):
    output_file = Path("data/processed/incidents.json")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(incidents, file, indent=4)

    print(f"Saved {len(incidents)} incidents to {output_file}")




def main():
    parsed_count = 0
    failed_count = 0
    records = []

    print("Parsing application logs...\n")

    with LOG_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if not line:
                continue

            record = parse_log_line(line)

            if record is None:
                failed_count += 1
                print("Could not parse:", line)
                continue

            parsed_count += 1
            records.append(record)
            print(record)

    print("\nParsing summary")
    print("Successfully parsed:", parsed_count)
    print("Failed to parse:", failed_count)


    # Detect incidents using parsed records
    incidents = detect_incidents(records)

    # Calculate and display incident summary
    summary = summarize_incidents(incidents)

    print("\nIncident Summary")
    print("----------------")

    for name, value in summary.items():
        print(f"{name.replace('_', ' ').title()}: {value}")

    # Save detected incidents to a JSON file
    save_incidents(incidents)
    save_incidents_to_db(incidents)

    print("\nIncident Detection Report")
    print("-------------------------")

    if not incidents:
        print("No incidents detected.")
    else:
        for incident in incidents:
            print("Incident ID:", incident["incident_id"])
            print("Title:", incident["title"])
            print("Severity:", incident["severity"])
            print("Pipeline:", incident["pipeline"])
            print("Error Count:", incident["error_count"])
            print("Error Code:", incident["error_code"])
            print("Status:", incident["status"])
            print("First Seen:", incident["first_seen"])
            print("Last Seen:", incident["last_seen"])
            print("Evidence:")

            for item in incident["evidence"]:
                print(
                    "  -",
                    item["timestamp"],
                    "|",
                    item["message"]
                )

        print("-------------------------")

if __name__ == "__main__":
    main()
