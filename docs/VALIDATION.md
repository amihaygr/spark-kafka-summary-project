# Validation Report

Date: 2026-08-08

Environment:

- Spark 3.4.0 / Scala 2.12
- Kafka `wurstmeister/kafka:2.13-2.8.1`
- MinIO `RELEASE.2022-11-08T05-27-07Z`
- Spark Kafka connector `spark-sql-kafka-0-10_2.12:3.4.0`

## Results

- Unit tests: 3/3 passed.
- `car_models`: 7 rows written to `s3a://spark/data/dims/car_models`.
- `car_colors`: 7 rows written to `s3a://spark/data/dims/car_colors`.
- `cars`: 20 unique cars written to `s3a://spark/data/dims/cars`.
- `sensors-sample`: 540 generated JSON events.
- `samples-enriched`: 540 enriched JSON events.
- `alert-data`: 499 alert events.
- Every row in `alert-data` independently passed at least one alert condition.
- Three Spark streaming applications and the generator were also started together successfully after non-project containers were stopped.

## Final aggregate cross-check

The Spark console result for windows containing all 499 alerts matched an independent calculation from the raw Kafka messages:

| Metric | Value |
|---|---:|
| `num_of_rows` | 499 |
| `num_of_black` | 50 |
| `num_of_white` | 73 |
| `num_of_silver` (Gray/Grey/Silver) | 73 |
| `maximum_speed` | 199 |
| `maximum_gear` | 7 |
| `maximum_rpm` | 7998 |

No source-code exceptions were present in the successful sequential validation runs.

