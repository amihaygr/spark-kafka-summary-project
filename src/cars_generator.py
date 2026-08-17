"""EX3: generate 20 cars and store them in MinIO/S3."""

from __future__ import annotations

import random

from pyspark.sql import types as T

from settings import S3_CARS_PATH
from spark_utils import build_spark


CAR_COUNT = 20
SCHEMA = T.StructType(
    [
        T.StructField("car_id", T.LongType(), False),
        T.StructField("driver_id", T.LongType(), False),
        T.StructField("model_id", T.IntegerType(), False),
        T.StructField("color_id", T.IntegerType(), False),
    ]
)


def generate_unique_numbers(count: int, start: int, end: int) -> list[int]:
    return random.sample(range(start, end + 1), count)


def generate_cars(count: int = CAR_COUNT) -> list[tuple[int, int, int, int]]:
    car_ids = generate_unique_numbers(count, 1_000_000, 9_999_999)
    driver_ids = generate_unique_numbers(count, 100_000_000, 999_999_999)
    return [
        (car_id, driver_id, random.randint(1, 7), random.randint(1, 7))
        for car_id, driver_id in zip(car_ids, driver_ids)
    ]


def main() -> None:
    spark = build_spark("CarsGenerator")
    try:
        dataframe = spark.createDataFrame(generate_cars(), SCHEMA)
        dataframe.write.mode("overwrite").parquet(S3_CARS_PATH)
        dataframe.orderBy("car_id").show(CAR_COUNT, truncate=False)
        print(f"Wrote {dataframe.count()} cars to {S3_CARS_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

