"""Kafka sources for the streaming queries.

Offsets are tracked by Spark in each query's checkpoint, not by Kafka consumer groups:
on restart a query resumes exactly where its last committed micro-batch ended.
``maxOffsetsPerTrigger`` bounds the size of a micro-batch, which keeps latency stable
during catch-up after an outage (backpressure).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from config.kafka_config import KAFKA
from config.spark_config import SPARK


def read_topic(spark: SparkSession, topic: str, max_offsets: int | None = None) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", SPARK.starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets or SPARK.max_offsets_per_trigger)
        # Retention may delete data while the pipeline is down for a long time; prefer
        # continuing with a gap (and a warning) over a crashed query.
        .option("failOnDataLoss", "false")
        .load()
    )
