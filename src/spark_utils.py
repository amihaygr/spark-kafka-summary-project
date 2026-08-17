"""Spark session and Kafka serialization helpers."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from settings import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    SPARK_LOG_LEVEL,
    SPARK_MASTER,
)


def build_spark(app_name: str) -> SparkSession:
    """Build a Spark session configured for the course MinIO service."""

    builder = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return spark


def kafka_json_payload(dataframe: DataFrame, key_column: str) -> DataFrame:
    """Convert all dataframe fields to Kafka key/value columns."""

    return dataframe.select(
        F.col(key_column).cast("string").alias("key"),
        F.to_json(F.struct(*[F.col(name) for name in dataframe.columns])).alias(
            "value"
        ),
    )
