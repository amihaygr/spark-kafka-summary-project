"""Create the MinIO bucket and Kafka topics required by the project."""

from __future__ import annotations

import sys
from pathlib import Path

import boto3
from botocore.client import Config
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from settings import (  # noqa: E402
    ALERT_TOPIC,
    ENRICHED_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    S3_BUCKET,
    SENSORS_TOPIC,
)


def ensure_bucket() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    buckets = {item["Name"] for item in s3.list_buckets().get("Buckets", [])}
    if S3_BUCKET not in buckets:
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Created MinIO bucket: {S3_BUCKET}")
    else:
        print(f"MinIO bucket already exists: {S3_BUCKET}")


def ensure_topics() -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="spark-kafka-summary-bootstrap",
    )
    try:
        existing = set(admin.list_topics())
        topics = [SENSORS_TOPIC, ENRICHED_TOPIC, ALERT_TOPIC]
        missing = [
            NewTopic(name=name, num_partitions=1, replication_factor=1)
            for name in topics
            if name not in existing
        ]
        if missing:
            try:
                admin.create_topics(missing)
            except TopicAlreadyExistsError:
                pass
            print("Created Kafka topics: " + ", ".join(item.name for item in missing))
        else:
            print("All Kafka topics already exist")
    finally:
        admin.close()


if __name__ == "__main__":
    ensure_bucket()
    ensure_topics()

