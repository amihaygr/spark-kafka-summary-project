"""EX1: create the car model dimension in MinIO/S3."""

from pyspark.sql import types as T

from settings import S3_MODELS_PATH
from spark_utils import build_spark


MODELS = [
    (1, "Mazda", "3"),
    (2, "Mazda", "6"),
    (3, "Toyota", "Corolla"),
    (4, "Hyundai", "i20"),
    (5, "Kia", "Sportage"),
    (6, "Kia", "Rio"),
    (7, "Kia", "Picanto"),
]

SCHEMA = T.StructType(
    [
        T.StructField("model_id", T.IntegerType(), False),
        T.StructField("car_brand", T.StringType(), False),
        T.StructField("car_model", T.StringType(), False),
    ]
)


def main() -> None:
    spark = build_spark("ModelCreation")
    try:
        dataframe = spark.createDataFrame(MODELS, SCHEMA)
        dataframe.write.mode("overwrite").parquet(S3_MODELS_PATH)
        dataframe.orderBy("model_id").show(truncate=False)
        print(f"Wrote {dataframe.count()} models to {S3_MODELS_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

