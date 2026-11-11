"""Pure DataFrame -> DataFrame transformations (unit-testable with batch DataFrames)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from producer.event_schema import NUMERIC_FIELDS
from streaming.validation import num, with_reject_reason

# Raw payloads are parsed with every field as STRING. Spark then decides what is valid
# itself; a typed schema would silently turn "ERR_SENSOR" into NULL and hide the error.
RAW_SCHEMA = StructType(
    [StructField(n, StringType()) for n in
     ("event_id", "device_id", "plant_id", "event_time", *NUMERIC_FIELDS, "status", "schema_version")]
    + [StructField("_corrupt_record", StringType())]
)

VALIDATED_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("device_id", StringType()),
        StructField("plant_id", StringType()),
        StructField("event_time", TimestampType()),
        *[StructField(n, DoubleType()) for n in NUMERIC_FIELDS],
        StructField("status", StringType()),
        StructField("ingest_time", TimestampType()),
        StructField("validated_at", TimestampType()),
    ]
)

JSON_OPTS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}


def parse_raw(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows -> parsed payload ``j`` + Kafka metadata + ``reject_reason``."""
    parsed = kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("ingest_time"),
    ).withColumn("j", F.from_json("raw_value", RAW_SCHEMA, JSON_OPTS))
    return with_reject_reason(parsed)


def to_validated(parsed: DataFrame) -> DataFrame:
    """Valid records as typed columns."""
    return parsed.where(F.col("reject_reason").isNull()).select(
        F.col("j.event_id").alias("event_id"),
        F.col("j.device_id").alias("device_id"),
        F.coalesce(F.col("j.plant_id"), F.lit("unknown")).alias("plant_id"),
        F.col("j.event_time").try_cast("timestamp").alias("event_time"),
        *[num(n).alias(n) for n in NUMERIC_FIELDS],
        F.coalesce(F.col("j.status"), F.lit("running")).alias("status"),
        "ingest_time",
        F.current_timestamp().alias("validated_at"),
    )


def to_rejected(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("reject_reason").isNotNull()).select(
        F.concat_ws("-", F.lit("raw-telemetry"), "kafka_partition", "kafka_offset").alias("id"),
        F.col("ingest_time").alias("received_at"),
        F.col("reject_reason").alias("reason"),
        F.col("raw_value").alias("raw_payload"),
        "kafka_partition",
        "kafka_offset",
    )


def as_kafka_json(df: DataFrame, key_col: str | None = "device_id") -> DataFrame:
    """Serialize every column of ``df`` into a Kafka (key, value) pair."""
    key = F.col(key_col).cast("string") if key_col else F.lit(None).cast("string")
    return df.select(key.alias("key"), F.to_json(F.struct(*df.columns)).alias("value"))


def parse_validated(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        F.from_json(F.col("value").cast("string"), VALIDATED_SCHEMA).alias("e")
    ).select("e.*").where(F.col("event_time").isNotNull() & F.col("event_id").isNotNull())


def deduplicate(events: DataFrame, watermark: str) -> DataFrame:
    """Event-time watermark + duplicate removal on ``event_id``.

    ``dropDuplicatesWithinWatermark`` keeps state only for as long as a duplicate could
    still arrive (the watermark delay), so state stays bounded no matter how long the
    pipeline runs. Events older than the watermark are dropped as too late.
    """
    return events.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(["event_id"])
