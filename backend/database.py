
import json
import sqlite3
from pathlib import Path

# Find the root folder of the project
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Store the database inside the data folder
DATA_DIR = PROJECT_ROOT / "data"
DATABASE_FILE = DATA_DIR / "incidents.db"


def get_connection():
    """Create and return a connection to the SQLite database."""

    # Make sure the data folder exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to the database
    connection = sqlite3.connect(DATABASE_FILE)

    # Allow database rows to be accessed by column name
    connection.row_factory = sqlite3.Row

    return connection

def initialize_database():
    """Create the incidents table if it does not already exist."""

    connection = get_connection()

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                severity TEXT NOT NULL,
                pipeline TEXT NOT NULL,
                error_count INTEGER NOT NULL,
                error_code TEXT NOT NULL,
                status TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                evidence_json TEXT NOT NULL
            )
        """)

        connection.commit()
        print("Incidents table created or already exists.")

    finally:
        connection.close()


def save_incidents(incidents):
    """Save incidents to the SQLite database."""

    initialize_database()
    connection = get_connection()

    try:
        connection.executemany(
            """
            INSERT OR REPLACE INTO incidents (
                incident_id,
                title,
                severity,
                pipeline,
                error_count,
                error_code,
                status,
                first_seen,
                last_seen,
                evidence_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    incident["incident_id"],
                    incident["title"],
                    incident["severity"],
                    incident["pipeline"],
                    incident["error_count"],
                    incident["error_code"],
                    incident["status"],
                    incident["first_seen"],
                    incident["last_seen"],
                    json.dumps(incident.get("evidence", [])),
                )
                for incident in incidents
            ],
        )

        connection.commit()
        print(f"Saved {len(incidents)} incident(s) to SQLite.")

    finally:
        connection.close()
