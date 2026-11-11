"""StreamPulse streaming application: three independent Structured Streaming queries.

    raw-telemetry --(validator)--> validated-telemetry --(aggregator)--> telemetry_metrics
                         |                         \\-----(detector)----> alerts + anomaly-events
                         \\--> invalid-telemetry + dead_letter_events

Each query has its own checkpoint (Kafka offsets + state). A failure or restart of one
query never forces the others to reprocess.

Run with ``spark-submit streaming/main.py [--queries validator,aggregator,detector]``.
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.kafka_config import KAFKA
from config.spark_config import SPARK
from streaming import sinks
from streaming.detection_stage import detection_stream
from streaming.kafka_consumer import read_topic
from streaming.transformations import (
    as_kafka_json,
    deduplicate,
    parse_raw,
    parse_validated,
    to_rejected,
    to_validated,
)
from streaming.windowing import window_aggregates

log = logging.getLogger("streampulse.streaming")


STATE_STORE_PROVIDERS = {
    # In-memory maps + delta files: lowest per-batch overhead for small state (default).
    "hdfs": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    # Off-heap, disk-backed: the right choice once state outgrows executor memory.
    "rocksdb": "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
}


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName(SPARK.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", SPARK.shuffle_partitions)
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.streaming.stateStore.providerClass", STATE_STORE_PROVIDERS[SPARK.state_store])
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    if SPARK.state_store == "rocksdb":
        # Changelog checkpointing makes commits incremental instead of full snapshots.
        builder = builder.config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
    return builder.getOrCreate()


# ------------------------------------------------------------------ query 1: validator
def start_validator(spark: SparkSession):
    parsed = parse_raw(read_topic(spark, KAFKA.topics.raw))
    name = "validator"

    def process(batch: DataFrame, batch_id: int) -> None:
        batch.persist()
        try:
            valid = to_validated(batch)
            rejected = to_rejected(batch)
            # Event volume stays distributed: one Kafka job for both outputs, where the
            # per-row `topic` column routes each record.
            sinks.write_kafka(
                as_kafka_json(valid).withColumn("topic", F.lit(KAFKA.topics.validated)).unionByName(
                    as_kafka_json(rejected, key_col=None).withColumn("topic", F.lit(KAFKA.topics.invalid))
                ),
                topic=None,
            )
            is_valid = F.col("reject_reason").isNull()
            stats = batch.agg(
                F.count(F.when(is_valid, 1)).alias("valid"),
                F.count(F.when(~is_valid, 1)).alias("rejected"),
                F.avg(sinks.ms_since("ingest_time")).alias("proc_ms"),
                F.avg(F.when(is_valid, (F.unix_micros(F.current_timestamp())
                                        - F.unix_micros(F.col("j.event_time").try_cast("timestamp"))) / 1000.0)
                      ).alias("e2e_ms"),
            ).first()
            if stats["rejected"]:
                sinks.write_rows([(sinks.DEAD_LETTER_SQL, sinks.DEAD_LETTER_COLUMNS, rejected.collect())])
            sinks.write_pipeline_metrics(
                name, batch_id,
                events_processed=int(stats["valid"]),
                events_rejected=int(stats["rejected"]),
                processing_latency_ms=stats["proc_ms"],
                end_to_end_latency_ms=stats["e2e_ms"],
            )
        finally:
            batch.unpersist()

    return (
        parsed.writeStream.queryName(name)
        .foreachBatch(process)
        .option("checkpointLocation", SPARK.checkpoint(name))
        .trigger(processingTime=SPARK.validator_trigger)
        .start()
    )


# ----------------------------------------------------------------- query 2: aggregator
def start_aggregator(spark: SparkSession):
    events = deduplicate(parse_validated(read_topic(spark, KAFKA.topics.validated)), SPARK.watermark_delay)
    windows = window_aggregates(events, SPARK.window_duration, SPARK.slide_duration)
    name = "aggregator"

    def process(batch: DataFrame, batch_id: int) -> None:
        # Output = windows updated in this batch: bounded by devices x open windows.
        collected = batch.withColumn("event_time", F.col("window_end")).collect()
        # One row per upsert key: Postgres rejects an INSERT ... ON CONFLICT DO UPDATE that
        # touches the same row twice (CardinalityViolation), which would crash-loop the query.
        by_key: dict[tuple, object] = {}
        for r in collected:
            key = (r["device_id"], r["window_start"], r["window_end"])
            if key not in by_key or r["event_count"] > by_key[key]["event_count"]:
                by_key[key] = r
        rows = list(by_key.values())
        last_seen: dict[str, dict] = {}
        for r in rows:
            cur = last_seen.get(r["device_id"])
            if cur is None or r["window_end"] > cur["last_seen_at"]:
                last_seen[r["device_id"]] = {"device_id": r["device_id"], "plant_id": r["plant_id"],
                                             "last_seen_at": r["window_end"]}
        device_rows = list(last_seen.values())
        sinks.write_rows([
            (sinks.TELEMETRY_SQL, sinks.TELEMETRY_COLUMNS, rows),
            (sinks.DEVICE_SQL, ["device_id", "plant_id", "last_seen_at"], device_rows),
        ])
        sinks.write_pipeline_metrics(name, batch_id, events_processed=len(rows), active_devices=len(last_seen))

    return (
        windows.writeStream.queryName(name)
        .outputMode("update")
        .foreachBatch(process)
        .option("checkpointLocation", SPARK.checkpoint(name))
        .trigger(processingTime=SPARK.aggregator_trigger)
        .start()
    )


# ------------------------------------------------------------------- query 3: detector
def start_detector(spark: SparkSession):
    events = deduplicate(parse_validated(read_topic(spark, KAFKA.topics.validated)), SPARK.watermark_delay)
    results = detection_stream(events)
    name = "detector"

    def process(batch: DataFrame, batch_id: int) -> None:
        # Output = alerts + one stats row per device: bounded by fleet size.
        batch.persist()
        try:
            rows = batch.collect()
            alerts = [r for r in rows if r["record_type"] == "alert"]
            stats = [r for r in rows if r["record_type"] == "stats"]
            sinks.write_rows([(sinks.ALERT_SQL, sinks.ALERT_COLUMNS, alerts)])
            if alerts:
                sinks.write_kafka(
                    as_kafka_json(batch.where(F.col("record_type") == "alert").select(*sinks.ALERT_COLUMNS)),
                    KAFKA.topics.anomalies,
                )
            events = sum(r["events_in_batch"] or 0 for r in stats)
            lag = (sum((r["lag_ms"] or 0) * (r["events_in_batch"] or 0) for r in stats) / events) if events else None
            sinks.write_pipeline_metrics(
                name, batch_id,
                events_processed=events,
                alerts_generated=len(alerts),
                active_devices=len(stats),
                end_to_end_latency_ms=lag,
            )
        finally:
            batch.unpersist()

    return (
        results.writeStream.queryName(name)
        .outputMode("append")
        .foreachBatch(process)
        .option("checkpointLocation", SPARK.checkpoint(name))
        .trigger(processingTime=SPARK.detector_trigger)
        .start()
    )


STARTERS = {"validator": start_validator, "aggregator": start_aggregator, "detector": start_detector}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries", default="validator,aggregator,detector")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    sinks.register_listener(spark)
    for name in [q.strip() for q in args.queries.split(",") if q.strip()]:
        STARTERS[name](spark)
        log.info("started query %s", name)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
