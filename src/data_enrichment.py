"""EX5: enrich sensor events with the static S3 dimensions."""

from pyspark.sql import functions as F

from schemas import SENSOR_SCHEMA
from settings import (
    ENRICHED_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    S3_CARS_PATH,
    S3_COLORS_PATH,
    S3_MODELS_PATH,
    SENSORS_TOPIC,
    checkpoint_path,
)
from spark_utils import build_spark, kafka_json_payload


def main() -> None:
    spark = build_spark("DataEnrichment")

    cars = spark.read.parquet(S3_CARS_PATH)
    models = spark.read.parquet(S3_MODELS_PATH)
    colors = spark.read.parquet(S3_COLORS_PATH)

    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", SENSORS_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    events = (
        raw_events.select(
            F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("event")
        )
        .select("event.*")
        .filter(F.col("event_id").isNotNull())
    )

    enriched = (
        events.join(F.broadcast(cars), "car_id", "inner")
        .join(F.broadcast(models), "model_id", "inner")
        .join(F.broadcast(colors), "color_id", "inner")
        .select(
            "event_id",
            "event_time",
            "car_id",
            "speed",
            "rpm",
            "gear",
            "driver_id",
            F.col("car_brand").alias("brand_name"),
            F.col("car_model").alias("model_name"),
            "color_name",
            F.round(F.col("speed") / F.lit(30.0)).cast("int").alias(
                "expected_gear"
            ),
        )
    )

    query = (
        kafka_json_payload(enriched, "car_id")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", ENRICHED_TOPIC)
        .option("checkpointLocation", checkpoint_path("data_enrichment"))
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
