"""Environment-based configuration shared by all project jobs."""

from __future__ import annotations

import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "course-kafka:9092"
)
SENSORS_TOPIC = os.getenv("SENSORS_TOPIC", "sensors-sample")
ENRICHED_TOPIC = os.getenv("ENRICHED_TOPIC", "samples-enriched")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "alert-data")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "spark")

S3_MODELS_PATH = os.getenv(
    "S3_MODELS_PATH", f"s3a://{S3_BUCKET}/data/dims/car_models"
)
S3_COLORS_PATH = os.getenv(
    "S3_COLORS_PATH", f"s3a://{S3_BUCKET}/data/dims/car_colors"
)
S3_CARS_PATH = os.getenv(
    "S3_CARS_PATH", f"s3a://{S3_BUCKET}/data/dims/cars"
)
CHECKPOINT_BASE = os.getenv(
    "CHECKPOINT_BASE", f"s3a://{S3_BUCKET}/data/checkpoints/spark_kafka_summary"
).rstrip("/")

SPARK_KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
)
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[1]")
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL", "WARN")
ALERT_WINDOW = os.getenv("ALERT_WINDOW", "15 minutes")
ALERT_SLIDE = os.getenv("ALERT_SLIDE", "1 minute")


def checkpoint_path(job_name: str) -> str:
    """Return a stable checkpoint path for a streaming job."""

    return f"{CHECKPOINT_BASE}/{job_name}"
