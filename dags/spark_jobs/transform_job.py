#!/usr/bin/env python3
"""
Minimal PySpark transform job to be launched by KubernetesPodOperator.

This script:
- Creates a SparkSession
- Parses --input and --output arguments
- Reads data (CSV/JSON/Parquet depending on your needs)
- Applies a trivial transformation
- Writes the result

Adjust I/O formats and options to your real sources/targets.

When running in Kubernetes with spark-submit (cluster mode), ensure that:
- The container image contains Spark and this script (mounted or baked into the image).
- Access to storage (S3/GCS/PVC) is configured via credentials and proper Spark configs.
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def build_spark(app_name: str = "etl-transform") -> SparkSession:
    """Create a SparkSession with sensible defaults.
    Add any config (e.g., Delta, S3 credentials) as needed.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        # .config("spark.sql.shuffle.partitions", "200")
        # Example: S3 credentials via env/secret
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .getOrCreate()
    )
    return spark


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Simple PySpark transform job")
    parser.add_argument("--input", required=True, help="Input path (e.g., s3a://bucket/raw or /mnt/raw)")
    parser.add_argument("--output", required=True, help="Output path (e.g., s3a://bucket/curated or /mnt/curated)")
    parser.add_argument("--format", default="parquet", choices=["parquet", "csv", "json"], help="Input/output format")
    return parser.parse_args(argv)


def run_job(input_path: str, output_path: str, fmt: str = "parquet") -> None:
    spark = build_spark()

    # Read
    if fmt == "parquet":
        df = spark.read.parquet(input_path)
    elif fmt == "csv":
        df = spark.read.option("header", True).csv(input_path)
    else:
        df = spark.read.json(input_path)

    # Transform (example: select columns and add a simple derived column)
    if "value" in df.columns:
        df = df.withColumn("value_doubled", col("value") * 2)

    # Write (overwrite for idempotency in examples; change to append if needed)
    if fmt == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif fmt == "csv":
        df.write.mode("overwrite").option("header", True).csv(output_path)
    else:
        df.write.mode("overwrite").json(output_path)

    spark.stop()


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    run_job(args.input, args.output, args.format)
