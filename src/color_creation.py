"""EX2: create the car color dimension in MinIO/S3."""

from pyspark.sql import types as T

from settings import S3_COLORS_PATH
from spark_utils import build_spark


COLORS = [
    (1, "Black"),
    (2, "Red"),
    (3, "Gray"),
    (4, "White"),
    (5, "Green"),
    (6, "Blue"),
    (7, "Pink"),
]

SCHEMA = T.StructType(
    [
        T.StructField("color_id", T.IntegerType(), False),
        T.StructField("color_name", T.StringType(), False),
    ]
)


def main() -> None:
    spark = build_spark("ColorCreation")
    try:
        dataframe = spark.createDataFrame(COLORS, SCHEMA)
        dataframe.write.mode("overwrite").parquet(S3_COLORS_PATH)
        dataframe.orderBy("color_id").show(truncate=False)
        print(f"Wrote {dataframe.count()} colors to {S3_COLORS_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

