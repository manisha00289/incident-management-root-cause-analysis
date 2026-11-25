
import os
import sys

# Ensure Spark uses the Python interpreter from the active environment.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


def main():
    # Start Spark locally using all available CPU cores.
    spark = (
        SparkSession.builder
        .appName("IncidentManagementSparkTest")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.pyspark.python", sys.executable)
.config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Create a small sample dataset.
    data = [
        (1, "Normal", 12),
        (2, "Anomaly", 25),
        (3, "Normal", 8),
        (4, "Anomaly", 30),
    ]

    columns = ["block_id", "status", "log_count"]

    df = spark.createDataFrame(data, columns)

    print("\n--- Spark DataFrame ---")
    df.show()

    print("\n--- Total Records ---")
    print(df.count())

    print("\n--- Records Grouped by Status ---")
    df.groupBy("status").count().orderBy("status").show()

    spark.stop()
    print("Spark application completed successfully.")


if __name__ == "__main__":
    main()
