
import os
import sys
from pathlib import Path
from backend.event_matcher import (
    load_event_templates,
    match_event_template,
)
from backend.block_processor import EVENT_IDS

# Configure Spark to use this project's Python environment.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Local HDFS benchmark dataset location.
HDFS_LOG_PATH = Path(
    r"C:\datasets\HDFS\extracted\HDFS.log"
)
HDFS_TEMPLATE_PATH = Path(
    r"C:\datasets\HDFS\extracted\preprocessed\HDFS.log_templates.csv"
)
# Regex pattern matching the existing Phase 1 HDFS parser.
LOG_PATTERN = (
    r"^(\d{6})\s+"
    r"(\d{6})\s+"
    r"(\d+)\s+"
    r"([A-Z]+)\s+"
    r"([^:]+):\s*"
    r"(.*)$"
)

# Identifies HDFS block IDs in log messages.
BLOCK_PATTERN = r"\bblk_-?\d+\b"


def create_spark_session() -> SparkSession:
    """Create a local Spark session."""

    return (
        SparkSession.builder
        .appName("IncidentManagementHDFSProcessor")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_and_parse_hdfs_logs(
    spark: SparkSession,
    log_path: Path
):
    """
    Read raw HDFS logs and extract structured fields
    using native Spark DataFrame functions.
    """

    if not log_path.is_file():
        raise FileNotFoundError(
            f"HDFS log file not found: {log_path}"
        )

    # Convert the Windows path into a file URI.
    file_uri = log_path.resolve().as_uri()

    # Read the text file as a Spark DataFrame.
    raw_df = spark.read.text(file_uri)

    # Match Phase 1 behavior by trimming each input line.
    parsed_df = raw_df.select(
        F.trim(F.col("value")).alias("raw_line")
    )

    # Extract fields using the same six regex groups
    # as backend/hdfs_parser.py.
    field_names = [
        "date_raw",
        "time_raw",
        "process_id_raw",
        "level",
        "component",
        "message",
    ]

    for index, field_name in enumerate(field_names, start=1):
        parsed_df = parsed_df.withColumn(
            field_name,
            F.regexp_extract(
                F.col("raw_line"),
                LOG_PATTERN,
                index
            )
        )

    # This benchmark uses year 2008.
    # Prefixing 20 preserves the Python parser's
    # interpretation of dates such as 081109.
    parsed_df = parsed_df.withColumn(
        "timestamp",
        F.to_timestamp(
            F.concat(
                F.lit("20"),
                F.col("date_raw"),
                F.lit(" "),
                F.col("time_raw")
            ),
            "yyyyMMdd HHmmss"
        )
    )

    parsed_df = parsed_df.withColumn(
        "process_id",
        F.col("process_id_raw").cast("long")
    )

    parsed_df = parsed_df.withColumn(
        "component",
        F.trim(F.col("component"))
    )

    # Extract all block IDs and remove duplicates
    # within each log record.
    parsed_df = parsed_df.withColumn(
        "block_ids",
        F.array_distinct(
            F.regexp_extract_all(
                F.col("message"),
                F.lit(BLOCK_PATTERN),
                0
            )
        )
    )

    # Identify malformed records and invalid timestamps.
    parsed_df = parsed_df.withColumn(
        "parse_status",
        F.when(
            F.col("date_raw") == "",
            F.lit("MALFORMED_FORMAT")
        )
        .when(
            F.col("timestamp").isNull(),
            F.lit("INVALID_TIMESTAMP")
        )
        .otherwise(F.lit("PARSED"))
    )

    # Return only the fields needed for downstream processing.
    return parsed_df.select(
        "raw_line",
        "timestamp",
        "process_id",
        "level",
        "component",
        "message",
        "block_ids",
        "parse_status",
    )
def add_event_template_matches(parsed_df, template_path: Path):
    """
    Match parsed HDFS messages against the event template catalog
    using native Spark regular-expression expressions.
    """

    templates = load_event_templates(template_path)

    if len(templates) != 29:
        raise ValueError(
            f"Expected 29 event templates, found {len(templates)}"
        )

    match_expressions = [
        F.when(
            (F.col("parse_status") == "PARSED")
            & F.col("message").rlike(template.pattern.pattern),
            F.lit(template.event_id)
        )
        for template in templates
    ]

    matched_df = parsed_df.withColumn(
        "_matched_event_ids",
        F.array_compact(F.array(*match_expressions))
    )

    matched_df = matched_df.withColumn(
        "event_match_count",
        F.size("_matched_event_ids")
    )

    matched_df = matched_df.withColumn(
        "event_id",
        F.when(
            F.col("event_match_count") == 1,
            F.element_at("_matched_event_ids", 1)
        )
    )

    matched_df = matched_df.withColumn(
        "event_match_status",
        F.when(
            F.col("event_match_count") == 0,
            F.lit("UNMATCHED")
        )
        .when(
            F.col("event_match_count") == 1,
            F.lit("MATCHED")
        )
        .otherwise(F.lit("AMBIGUOUS"))
    )

    print(f"Loaded {len(templates)} event templates.")

    return matched_df

def validate_event_matching(matched_df, template_path: Path):
    """
    Compare Spark event matching results with
    the original Phase 1 Python event matcher.
    """

    # Load the original Python event templates.
    templates = load_event_templates(template_path)

    # Select 10 parsed records from Spark.
    sample = (
        matched_df
        .filter(F.col("parse_status") == "PARSED")
        .select("message", "event_id")
        .limit(1000)
        .collect()
    )

    mismatches = []

    # Compare the Python and Spark results.
    for row in sample:

        python_event_id = match_event_template(
            row["message"],
            templates
        )

        spark_event_id = row["event_id"]

        if python_event_id != spark_event_id:
            mismatches.append({
                "message": row["message"],
                "python_event_id": python_event_id,
                "spark_event_id": spark_event_id,
            })

    # Display the validation results.
    print("\n--- Python vs Spark Validation ---")
    print(f"Records compared: {len(sample)}")
    print(f"Mismatches: {len(mismatches)}")

    for mismatch in mismatches:
        print(mismatch)

    if mismatches:
        raise AssertionError(
            "Python and Spark event matching differ."
        )

    print("Python and Spark event matching agree.")
def aggregate_block_features(matched_df):
    """
    Aggregate log records into one feature row per HDFS block.
    """

    print("\nStarting block-level feature aggregation...")

    # Keep only successfully parsed records with exactly one event match
    valid_df = matched_df.filter(
        (F.col("parse_status") == "PARSED") &
        (F.col("event_match_status") == "MATCHED") &
        (F.size("block_ids") > 0)
    )

    # Create one row for each unique block ID in a log record
    exploded_df = valid_df.withColumn(
        "block_id",
        F.explode("block_ids")
    )

     # Create aggregation expressions for all 29 event types
    event_count_expressions = [
           F.sum(
               F.when(F.col("event_id") == event_id, 1).otherwise(0)
           ).alias(event_id)
           for event_id in EVENT_IDS
    ]

    # Aggregate log-level features for each block
    block_features_df = (
        exploded_df
        .groupBy("block_id")
        .agg(
            F.count("*").alias("log_count"),

            F.sum(
                F.when(F.col("level") == "INFO", 1).otherwise(0)
            ).alias("info_count"),

            F.sum(
                F.when(F.col("level") == "WARN", 1).otherwise(0)
            ).alias("warn_count"),

            F.min("timestamp").alias("first_seen"),
            F.max("timestamp").alias("last_seen"),
              
              *event_count_expressions
        )
    )

    print("Block-level aggregation completed.")

    return block_features_df

def main():

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\nStarting HDFS log processing...")

        parsed_df = read_and_parse_hdfs_logs(
            spark,
            HDFS_LOG_PATH
        )

        print("\n--- Parsed DataFrame Schema ---")
        parsed_df.printSchema()

        matched_df = add_event_template_matches(
            parsed_df,
            HDFS_TEMPLATE_PATH
        )
        validate_event_matching(
             matched_df,
             HDFS_TEMPLATE_PATH
         )

        block_features_df = aggregate_block_features(matched_df)

        print("\n--- First 10 Block Features ---")
        block_features_df.show(10, truncate=False)
       
        print("\n--- First 10 Event Matches ---")

        matched_df.select(
            "message",
            "event_id",
            "event_match_count",
            "event_match_status"
        ).show(10, truncate=False)

        print("\nSpark event matching sample completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()