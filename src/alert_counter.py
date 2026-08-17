"""EX7: print rolling 15-minute alert aggregates to the console."""

from pyspark.sql import functions as F

from schemas import ENRICHED_SCHEMA
from settings import (
    ALERT_SLIDE,
    ALERT_TOPIC,
    ALERT_WINDOW,
    KAFKA_BOOTSTRAP_SERVERS,
    checkpoint_path,
)
from spark_utils import build_spark


def main() -> None:
    spark = build_spark("AlertCounter")

    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", ALERT_TOPIC)
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
    normalized_color = F.lower(F.trim(F.col("color_name")))
    counters = (
        events.withWatermark("event_time", "1 minute")
        .groupBy(F.window("event_time", ALERT_WINDOW, ALERT_SLIDE))
        .agg(
            F.count("*").alias("num_of_rows"),
            F.sum(F.when(normalized_color == "black", 1).otherwise(0)).alias(
                "num_of_black"
            ),
            F.sum(F.when(normalized_color == "white", 1).otherwise(0)).alias(
                "num_of_white"
            ),
            F.sum(
                F.when(
                    normalized_color.isin("gray", "grey", "silver"), 1
                ).otherwise(0)
            ).alias("num_of_silver"),
            F.max("speed").alias("maximum_speed"),
            F.max("gear").alias("maximum_gear"),
            F.max("rpm").alias("maximum_rpm"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "num_of_rows",
            "num_of_black",
            "num_of_white",
            "num_of_silver",
            "maximum_speed",
            "maximum_gear",
            "maximum_rpm",
        )
    )

    query = (
        counters.writeStream.format("console")
        .option("truncate", "false")
        .option("numRows", "100")
        .option("checkpointLocation", checkpoint_path("alert_counter"))
        .outputMode("update")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
