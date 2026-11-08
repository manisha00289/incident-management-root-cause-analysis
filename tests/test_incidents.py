
from backend.main import (
    detect_incidents,
    determine_severity,
    summarize_incidents,
)


def test_repeated_errors_create_an_incident():
    records = [
        {
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "DB_CONN_TIMEOUT",
            "message": "Database timeout",
        },
        {
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "DB_CONN_TIMEOUT",
            "message": "Database timeout",
        },
        {
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "DB_CONN_TIMEOUT",
            "message": "Database timeout",
        },
    ]

    incidents = detect_incidents(records)

    assert len(incidents) == 1
    assert incidents[0]["pipeline"] == "customer_ingestion"
    assert incidents[0]["error_count"] == 3
    assert incidents[0]["error_code"] == "DB_CONN_TIMEOUT"

def test_different_pipelines_create_separate_incidents():
    records = [
        {
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "DB_CONN_TIMEOUT",
            "message": "Database timeout",
        },
        {
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "DB_CONN_TIMEOUT",
            "message": "Database timeout",
        },
        {
            "pipeline": "orders_pipeline",
            "severity": "ERROR",
            "error_code": "SCHEMA_MISMATCH",
            "message": "Unexpected column type",
        },
        {
            "pipeline": "orders_pipeline",
            "severity": "ERROR",
            "error_code": "SCHEMA_MISMATCH",
            "message": "Unexpected column type",
        },
    ]

    incidents = detect_incidents(records)

    assert len(incidents) == 2

    pipelines = {incident["pipeline"] for incident in incidents}

    assert pipelines == {"customer_ingestion", "orders_pipeline"}


def test_single_error_does_not_create_incident():
    records = [
        {
            "pipeline": "payments_pipeline",
            "severity": "ERROR",
            "error_code": "PAYMENT_TIMEOUT",
            "message": "Payment service timeout",
        }
    ]

    incidents = detect_incidents(records)

    assert incidents == []

def test_incident_severity_assignment():
    assert determine_severity("DB_CONN_TIMEOUT", 3) == "HIGH"
    assert determine_severity("SCHEMA_MISMATCH", 2) == "MEDIUM"
    assert determine_severity("PAYMENT_TIMEOUT", 1) == "LOW"

def test_incident_records_first_and_last_seen():
    records = [
        {
            "timestamp": "2026-10-20 08:01:15",
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "DB_CONN_TIMEOUT",
            "message": "Database timeout",
        },
        {
            "timestamp": "2026-10-20 08:01:25",
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "DB_CONN_TIMEOUT",
            "message": "Database timeout",
        },
    ]

    incidents = detect_incidents(records)

    assert len(incidents) == 1
    assert incidents[0]["first_seen"] == "2026-10-20 08:01:15"
    assert incidents[0]["last_seen"] == "2026-10-20 08:01:25"


def test_incident_summary_statistics():
    incidents = [
        {
            "severity": "HIGH",
            "error_count": 3,
            "pipeline": "customer_ingestion",
        },
        {
            "severity": "MEDIUM",
            "error_count": 2,
            "pipeline": "orders_pipeline",
        },
    ]

    summary = summarize_incidents(incidents)

    assert summary["total_incidents"] == 2
    assert summary["high_severity"] == 1
    assert summary["medium_severity"] == 1
    assert summary["low_severity"] == 0
    assert summary["total_errors"] == 5
    assert summary["affected_pipelines"] == 2


def test_empty_records_create_no_incidents():
    incidents = detect_incidents([])

    assert incidents == []


def test_different_error_codes_create_separate_incidents():
    records = [
        {
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "DB_CONN_TIMEOUT",
            "message": "Database timeout",
        },
        {
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "DB_CONN_TIMEOUT",
            "message": "Database timeout",
        },
        {
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "SCHEMA_MISMATCH",
            "message": "Schema mismatch",
        },
        {
            "pipeline": "customer_ingestion",
            "severity": "ERROR",
            "error_code": "SCHEMA_MISMATCH",
            "message": "Schema mismatch",
        },
    ]

    incidents = detect_incidents(records)

    assert len(incidents) == 2

    error_codes = {incident["error_code"] for incident in incidents}

    assert error_codes == {"DB_CONN_TIMEOUT", "SCHEMA_MISMATCH"}
