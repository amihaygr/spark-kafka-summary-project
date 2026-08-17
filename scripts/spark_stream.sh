#!/usr/bin/env bash
set -euo pipefail

PACKAGE="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0}"
DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-512m}"
exec spark-submit --driver-memory "$DRIVER_MEMORY" --packages "$PACKAGE" "$@"
