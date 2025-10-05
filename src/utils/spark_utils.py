import os
from typing import Dict, Optional
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "DataPipeline", extra_confs: Optional[Dict[str, str]] = None) -> SparkSession:
    """Create a SparkSession.

    If MINIO_* environment variables are present, configure s3a to talk to MinIO.
    Env vars used:
      - MINIO_ENDPOINT (e.g., http://minio:9000)
      - MINIO_ACCESS_KEY
      - MINIO_SECRET_KEY
      - MINIO_REGION (default: us-east-1)
      - MINIO_SSL_ENABLED ("true"/"false")
      - MINIO_PATH_STYLE ("true"/"false", default true)
    """

    builder = SparkSession.builder.appName(app_name)

    # Configure MinIO via s3a if env vars are present
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if endpoint and access_key and secret_key:
        region = os.getenv("MINIO_REGION", "us-east-1")
        ssl_enabled = os.getenv("MINIO_SSL_ENABLED", "false").lower() == "true"
        path_style = os.getenv("MINIO_PATH_STYLE", "true").lower() == "true"

        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
            .config("spark.hadoop.fs.s3a.path.style.access", str(path_style).lower())
            .config("spark.hadoop.fs.s3a.endpoint.region", region)
        )

    # Apply any extra configs provided by caller
    if extra_confs:
        for k, v in extra_confs.items():
            builder = builder.config(k, v)

    return builder.getOrCreate()

