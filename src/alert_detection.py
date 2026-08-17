"""EX6: retain events matching any alert condition."""

from pyspark.sql import functions as F

from schemas import ENRICHED_SCHEMA
from settings import (
    ALERT_TOPIC,
    ENRICHED_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    checkpoint_path,
)
from spark_utils import build_spark, kafka_json_payload


def main() -> None:
    spark = build_spark("AlertDetection")

    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", ENRICHED_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    events = (
        raw_events.select(
            F.from_json(F.col("value").cast("string"), ENRICHED_SCHEMA).alias(
                "event"
            )
        )
        .select("event.*")
        .filter(F.col("event_id").isNotNull())
    )
    alerts = events.filter(
        (F.col("speed") > 120)
        | (F.col("expected_gear") != F.col("gear"))
        | (F.col("rpm") > 6000)
    )

    query = (
        kafka_json_payload(alerts, "car_id")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", ALERT_TOPIC)
        .option("checkpointLocation", checkpoint_path("alert_detection"))
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
