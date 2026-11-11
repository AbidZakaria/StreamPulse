"""Sinks: PostgreSQL (idempotent upserts), Kafka and pipeline metrics.

Delivery semantics
------------------
``foreachBatch`` is at-least-once: if the driver dies after writing a batch but before
committing it, the batch is replayed with the same ``batch_id`` and the same data.

* PostgreSQL writes are idempotent (natural keys + ``ON CONFLICT``), so a replay is
  harmless: **effectively-once persistence**.
* Kafka sinks (``validated-telemetry``, ``invalid-telemetry``, ``anomaly-events``) are
  at-least-once and may receive duplicates on replay. Downstream consumers deduplicate
  on ``event_id`` / ``alert_id``, as our own aggregator and detector do.

The project does not claim end-to-end exactly-once.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from pyspark.sql import DataFrame, Row, SparkSession  # noqa: F401  (Row used in type hints)
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from config import postgres_config
from config.kafka_config import KAFKA

log = logging.getLogger("streampulse.sinks")


# --------------------------------------------------------------------------- postgres
def pg_connect():
    import psycopg2

    cfg = postgres_config()
    return psycopg2.connect(
        host=cfg.host, port=cfg.port, dbname=cfg.database, user=cfg.user, password=cfg.password,
        connect_timeout=10, options="-c timezone=UTC",
    )


def write_rows(statements: list[tuple[str, list[str], Iterable[Row]]], page_size: int = 1000) -> None:
    """Write several (sql, columns, rows) groups in ONE transaction from the driver.

    Only bounded outputs go through here: window aggregates (devices x open windows),
    alerts and rejected records. Raw event volume never reaches the driver; it stays on
    the executors (Kafka source -> Kafka sink). Doing these small writes from the driver
    replaced one Python-worker stage per table per batch and cut micro-batch time ~3x
    on the reference machine (see docs/benchmarking.md).
    """
    from psycopg2.extras import execute_values

    groups = [(sql, cols, [tuple(r[c] for c in cols) for r in rows]) for sql, cols, rows in statements]
    if not any(values for _, _, values in groups):
        return
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:   # one transaction: all or nothing, idempotent on replay
            for sql, _, values in groups:
                if values:
                    execute_values(cur, sql, values, page_size=page_size)
    finally:
        conn.close()


TELEMETRY_COLUMNS = [
    "device_id", "plant_id", "window_start", "window_end", "event_time",
    "avg_temperature", "min_temperature", "max_temperature", "std_temperature",
    "avg_vibration", "max_vibration", "std_vibration",
    "avg_pressure", "min_pressure", "avg_rpm", "avg_power_consumption",
    "event_count", "missing_measurement_rate",
]
_TM_UPDATES = ", ".join(f"{c} = EXCLUDED.{c}" for c in TELEMETRY_COLUMNS[5:])
TELEMETRY_SQL = f"""
    INSERT INTO telemetry_metrics ({", ".join(TELEMETRY_COLUMNS)}) VALUES %s
    ON CONFLICT (device_id, window_start, window_end)
    DO UPDATE SET {_TM_UPDATES}, updated_at = now()
"""

DEVICE_SQL = """
    INSERT INTO devices (device_id, plant_id, last_seen_at) VALUES %s
    ON CONFLICT (device_id) DO UPDATE
       SET last_seen_at = GREATEST(devices.last_seen_at, EXCLUDED.last_seen_at)
"""

ALERT_COLUMNS = [
    "alert_id", "device_id", "plant_id", "metric_name", "algorithm", "alert_kind",
    "observed_value", "baseline_value", "deviation_score", "severity", "description",
    "event_id", "detected_at",
]
ALERT_SQL = f"""
    INSERT INTO alerts ({", ".join(ALERT_COLUMNS)}) VALUES %s
    ON CONFLICT (alert_id) DO NOTHING
"""

DEAD_LETTER_COLUMNS = ["id", "received_at", "reason", "raw_payload", "kafka_partition", "kafka_offset"]
DEAD_LETTER_SQL = f"""
    INSERT INTO dead_letter_events ({", ".join(DEAD_LETTER_COLUMNS)}) VALUES %s
    ON CONFLICT (id) DO NOTHING
"""


def write_pipeline_metrics(query_name: str, batch_id: int, **values) -> None:
    """Driver-side upsert of one row per (query, batch)."""
    cols = ["query_name", "batch_id", *values]
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in values) or "batch_id = EXCLUDED.batch_id"
    sql = (
        f"INSERT INTO pipeline_metrics ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT (query_name, batch_id) DO UPDATE SET {updates}, recorded_at = now()"
    )
    try:
        conn = pg_connect()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(sql, [query_name, batch_id, *values.values()])
        finally:
            conn.close()
    except Exception:  # noqa: BLE001 - metrics must never fail the data path
        log.exception("could not write pipeline metrics for %s/%s", query_name, batch_id)


# ------------------------------------------------------------------------------ kafka
def write_kafka(df: DataFrame, topic: str | None) -> None:
    """Batch write to Kafka. With ``topic=None`` each row's ``topic`` column routes it."""
    writer = (
        df.write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA.bootstrap_servers)
        .option("kafka.acks", "all")
        .option("kafka.linger.ms", "5")
    )
    if topic is not None:
        writer = writer.option("topic", topic)
    writer.save()


def ms_since(col: str):
    return (F.unix_micros(F.current_timestamp()) - F.unix_micros(F.col(col))) / 1000.0


# ---------------------------------------------------------------------- query listener
class MetricsListener(StreamingQueryListener):
    """Copies Spark's own progress metrics (rates, batch duration) into pipeline_metrics."""

    def onQueryStarted(self, event):
        log.info("query started: %s (%s)", event.name, event.id)

    def onQueryProgress(self, event):
        p = event.progress
        if not p.name:
            return
        log.info("progress %s batch=%s rows=%s durations=%s", p.name, p.batchId, p.numInputRows,
                 dict(p.durationMs or {}))
        write_pipeline_metrics(
            p.name,
            p.batchId,
            input_rows_per_sec=float(p.inputRowsPerSecond or 0.0),
            processed_rows_per_sec=float(p.processedRowsPerSecond or 0.0),
            batch_duration_ms=int(p.batchDuration or 0),
        )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.warning("query terminated: %s exception=%s", event.id, event.exception)


def register_listener(spark: SparkSession) -> None:
    spark.streams.addListener(MetricsListener())
