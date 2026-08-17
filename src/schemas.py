"""Schemas for the JSON messages sent through Kafka."""

from pyspark.sql import types as T


SENSOR_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("event_time", T.TimestampType(), False),
        T.StructField("car_id", T.LongType(), False),
        T.StructField("speed", T.IntegerType(), False),
        T.StructField("rpm", T.IntegerType(), False),
        T.StructField("gear", T.IntegerType(), False),
    ]
)

ENRICHED_SCHEMA = T.StructType(
    SENSOR_SCHEMA.fields
    + [
        T.StructField("driver_id", T.LongType(), False),
        T.StructField("brand_name", T.StringType(), False),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("color_name", T.StringType(), False),
        T.StructField("expected_gear", T.IntegerType(), False),
    ]
)

