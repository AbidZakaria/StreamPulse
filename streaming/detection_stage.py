"""Stateful, distributed detection with ``applyInPandasWithState``.

The validated stream is grouped by ``device_id``. Spark routes every device to one task
per micro-batch, so detector state never has to be shared or locked, and devices are
processed in parallel across executors. Each device's detector state (CUSUM sums,
EWMA baselines, drift windows, alert cooldowns) is a JSON document kept in Spark's
``GroupState``. It is written to the RocksDB state store and checkpointed with the
Kafka offsets, so after a crash the detectors resume with exactly the state that matches
the last committed offsets.

The function emits two kinds of rows:

* ``alert``: one per emitted alert, persisted to PostgreSQL and published to Kafka.
* ``stats``: one per device per micro-batch (events processed, average lag), used for
  pipeline metrics.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from producer.event_schema import NUMERIC_FIELDS

OUTPUT_SCHEMA = StructType(
    [
        StructField("record_type", StringType()),
        StructField("alert_id", StringType()),
        StructField("device_id", StringType()),
        StructField("plant_id", StringType()),
        StructField("metric_name", StringType()),
        StructField("algorithm", StringType()),
        StructField("alert_kind", StringType()),
        StructField("observed_value", DoubleType()),
        StructField("baseline_value", DoubleType()),
        StructField("deviation_score", DoubleType()),
        StructField("severity", StringType()),
        StructField("description", StringType()),
        StructField("event_id", StringType()),
        StructField("detected_at_epoch", DoubleType()),
        StructField("events_in_batch", LongType()),
        StructField("lag_ms", DoubleType()),
    ]
)
STATE_SCHEMA = StructType([StructField("state_json", StringType())])
COLUMNS = [f.name for f in OUTPUT_SCHEMA.fields]

_ENGINE = None


def _engine():
    """One engine per Python worker process (config is read once)."""
    global _ENGINE
    if _ENGINE is None:
        from detection.engine import DetectionEngine

        _ENGINE = DetectionEngine.from_yaml()
    return _ENGINE


def detect_device(key: tuple, batches: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    engine = _engine()
    device_state = json.loads(state.get[0]) if state.exists else engine.new_device_state()

    frames = [b for b in batches if len(b)]
    if not frames:
        return
    pdf = pd.concat(frames, ignore_index=True).sort_values(["event_time_epoch", "event_id"])

    rows: list[dict] = []
    now = time.time()
    lag_total = 0.0
    for rec in pdf.itertuples(index=False):
        event = {
            "device_id": rec.device_id,
            "plant_id": rec.plant_id,
            "event_id": rec.event_id,
            "event_time": rec.event_time_epoch,
        }
        for m in NUMERIC_FIELDS:
            event[m] = getattr(rec, m)
        lag_total += now - rec.event_time_epoch
        _, alerts = engine.process_event(device_state, event)
        for alert in alerts:
            r = alert.to_record()
            rows.append(
                {
                    "record_type": "alert",
                    **{k: r[k] for k in ("alert_id", "device_id", "plant_id", "metric_name", "algorithm",
                                         "alert_kind", "observed_value", "baseline_value",
                                         "deviation_score", "severity", "description", "event_id")},
                    "detected_at_epoch": r["detected_at"],
                    "events_in_batch": None,
                    "lag_ms": None,
                }
            )

    state.update((json.dumps(device_state, separators=(",", ":")),))
    rows.append(
        {
            "record_type": "stats",
            "device_id": key[0],
            "events_in_batch": len(pdf),
            "lag_ms": 1000.0 * lag_total / len(pdf),
        }
    )
    yield pd.DataFrame(rows, columns=COLUMNS)


def detection_stream(events: DataFrame) -> DataFrame:
    """``events``: deduplicated, watermarked validated events."""
    prepared = events.select(
        "device_id",
        "plant_id",
        "event_id",
        (F.unix_micros("event_time") / 1_000_000.0).alias("event_time_epoch"),
        *NUMERIC_FIELDS,
    )
    return (
        prepared.groupBy("device_id")
        .applyInPandasWithState(
            detect_device,
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            # Detector state is bounded by fleet size; nothing to time out.
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
        .withColumn("detected_at", F.timestamp_seconds("detected_at_epoch"))
    )
