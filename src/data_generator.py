"""EX4: emit one random sensor event per car every second."""

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from settings import KAFKA_BOOTSTRAP_SERVERS, S3_CARS_PATH, SENSORS_TOPIC
from spark_utils import build_spark


def build_event(car_id: int) -> dict[str, object]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "car_id": car_id,
        "speed": random.randint(0, 200),
        "rpm": random.randint(0, 8000),
        "gear": random.randint(1, 7),
    }


def main() -> None:
    spark = build_spark("DataGenerator")
    try:
        car_ids = [row.car_id for row in spark.read.parquet(S3_CARS_PATH).select("car_id").collect()]
    finally:
        spark.stop()

    if not car_ids:
        raise RuntimeError(f"No cars found at {S3_CARS_PATH}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda value: str(value).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )
    print(f"Producing {len(car_ids)} events/second to {SENSORS_TOPIC}")
    try:
        while True:
            started_at = time.monotonic()
            for car_id in car_ids:
                producer.send(SENSORS_TOPIC, key=car_id, value=build_event(car_id))
            producer.flush()
            elapsed = time.monotonic() - started_at
            print(f"Sent {len(car_ids)} events at {datetime.now(timezone.utc).isoformat()}")
            time.sleep(max(0.0, 1.0 - elapsed))
    except KeyboardInterrupt:
        print("Stopping data generator")
    finally:
        producer.close()


if __name__ == "__main__":
    main()

