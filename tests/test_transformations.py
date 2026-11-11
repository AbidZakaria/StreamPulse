"""Spark transformation tests (batch DataFrames, local Spark).

Run inside the Spark image so no local Java/Spark install is needed::

    make test-spark
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from producer.anomaly_injector import MALFORMED_KINDS, corrupt  # noqa: E402
from producer.event_schema import validate_event  # noqa: E402
from streaming.transformations import (  # noqa: E402
    VALIDATED_SCHEMA,
    as_kafka_json,
    deduplicate,
    parse_raw,
    parse_validated,
    to_rejected,
    to_validated,
)
from streaming.windowing import window_aggregates  # noqa: E402

pytestmark = pytest.mark.spark

GOOD = {"event_id": "e1", "device_id": "sensor_001", "plant_id": "plant_01",
        "event_time": "2026-11-10T09:30:15.250Z", "temperature": 78.5, "vibration": 2.4,
        "pressure": 101.2, "rpm": 1450, "power_consumption": 4.8}

PAYLOADS = [
    json.dumps(GOOD),
    json.dumps({**GOOD, "temperature": None}),
    json.dumps({**GOOD, "temperature": "78.5"}),
    "{not json",
    "[1, 2, 3]",
    json.dumps({k: v for k, v in GOOD.items() if k != "device_id"}),
    json.dumps({**GOOD, "event_id": "  "}),
    json.dumps({**GOOD, "event_time": "31/02/2026 25:61"}),
    json.dumps({**GOOD, "temperature": "ERR_SENSOR"}),
    json.dumps({**GOOD, "rpm": True}),
    json.dumps({**GOOD, "rpm": -250}),
    json.dumps({**GOOD, "pressure": 9999}),
    json.dumps({**GOOD, **{k: None for k in ("temperature", "vibration", "pressure", "rpm", "power_consumption")}}),
    *[corrupt(GOOD, k) for k in MALFORMED_KINDS],
]


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("streampulse-tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", 1)
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def kafka_frame(spark, payloads):
    rows = [(p.encode(), 0, i, datetime(2026, 11, 10, 9, 30, 20)) for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows, "value binary, partition int, offset long, timestamp timestamp")


def test_spark_validation_matches_python_reference(spark):
    """Every payload gets the same verdict and reason in Spark and in validate_event()."""
    parsed = parse_raw(kafka_frame(spark, PAYLOADS))
    got = {r["kafka_offset"]: r["reject_reason"] for r in parsed.select("kafka_offset", "reject_reason").collect()}
    for i, payload in enumerate(PAYLOADS):
        ok, reason = validate_event(payload)
        assert got[i] == reason, f"payload {i}: spark={got[i]!r} python={reason!r} :: {payload}"


def test_validated_records_are_typed(spark):
    valid = to_validated(parse_raw(kafka_frame(spark, [json.dumps(GOOD), json.dumps({**GOOD, "rpm": "1500"})])))
    rows = valid.collect()
    assert len(rows) == 2
    assert rows[0]["event_time"] == datetime(2026, 11, 10, 9, 30, 15, 250000)
    assert isinstance(rows[0]["temperature"], float)
    assert rows[1]["rpm"] == 1500.0


def test_rejected_records_carry_position_and_reason(spark):
    rejected = to_rejected(parse_raw(kafka_frame(spark, [json.dumps(GOOD), "{oops"]))).collect()
    assert len(rejected) == 1
    assert rejected[0]["id"] == "raw-telemetry-0-1"
    assert rejected[0]["reason"] == "malformed_json"
    assert rejected[0]["raw_payload"] == "{oops"


def test_validated_json_roundtrip(spark):
    valid = to_validated(parse_raw(kafka_frame(spark, [json.dumps(GOOD)])))
    msg = as_kafka_json(valid).collect()[0]
    assert msg["key"] == "sensor_001"
    back = parse_validated(spark.createDataFrame([(msg["value"].encode(),)], "value binary")).collect()[0]
    assert back["event_id"] == "e1" and back["temperature"] == 78.5
    assert [f.name for f in VALIDATED_SCHEMA.fields] == list(back.asDict())


def test_deduplicate_drops_repeated_event_ids(spark, tmp_path):
    """dropDuplicatesWithinWatermark is streaming-only: run it as a real stream
    (file source -> memory sink, availableNow trigger)."""
    src = tmp_path / "in"
    src.mkdir()
    events = [{"event_id": "e1", "event_time": "2026-11-10T09:00:00Z"},
              {"event_id": "e1", "event_time": "2026-11-10T09:00:00Z"},   # duplicate
              {"event_id": "e2", "event_time": "2026-11-10T09:00:01Z"},
              {"event_id": "e1", "event_time": "2026-11-10T09:00:30Z"}]   # same id, later copy
    (src / "part-0.json").write_text("\n".join(json.dumps(e) for e in events))
    stream = spark.readStream.schema("event_id string, event_time timestamp").json(str(src))
    query = (deduplicate(stream, "2 minutes").writeStream.format("memory").queryName("dedup_test")
             .option("checkpointLocation", str(tmp_path / "ckpt")).trigger(availableNow=True).start())
    query.awaitTermination(120)
    ids = sorted(r["event_id"] for r in spark.sql("SELECT event_id FROM dedup_test").collect())
    assert ids == ["e1", "e2"]


def test_window_aggregates(spark):
    t0 = datetime(2026, 11, 10, 9, 0, 0)
    rows = []
    for i in range(60):
        rows.append(("sensor_001", "plant_01", t0 + timedelta(seconds=i), 60.0 + (i % 2), 2.0, 100.0, 1500.0, 4.0))
    rows.append(("sensor_001", "plant_01", t0 + timedelta(seconds=30), None, 2.0, 100.0, 1500.0, 4.0))
    df = spark.createDataFrame(
        rows, "device_id string, plant_id string, event_time timestamp, temperature double, vibration double, "
              "pressure double, rpm double, power_consumption double")
    out = window_aggregates(df, "1 minute", "15 seconds").where(F.col("window_start") == t0).collect()
    assert len(out) == 1
    w = out[0]
    assert w["event_count"] == 61
    assert w["min_temperature"] == 60.0 and w["max_temperature"] == 61.0
    assert w["avg_temperature"] == pytest.approx(60.5)
    assert w["missing_measurement_rate"] == pytest.approx(1 / (61 * 5))


def test_detect_device_emits_alerts_and_stats():
    """The applyInPandasWithState function, driven with a fake GroupState."""
    pd = pytest.importorskip("pandas")
    from streaming.detection_stage import COLUMNS, detect_device

    class FakeState:
        def __init__(self):
            self.value = None

        @property
        def exists(self):
            return self.value is not None

        @property
        def get(self):
            return self.value

        def update(self, v):
            self.value = v

    state = FakeState()
    base = 1_767_225_600.0
    rows = [{"device_id": "d1", "plant_id": "p", "event_id": f"e{i}", "event_time_epoch": base + i,
             "temperature": 60.0 + (0.5 if i % 2 else -0.5), "vibration": 2.0, "pressure": 100.0,
             "rpm": 1500.0, "power_consumption": 4.0} for i in range(400)]
    rows[350]["temperature"] = 130.0  # spike: threshold + z-score
    out = pd.concat(list(detect_device(("d1",), iter([pd.DataFrame(rows)]), state)))
    assert list(out.columns) == COLUMNS
    alerts = out[out.record_type == "alert"]
    assert {"threshold", "zscore"} <= set(alerts.algorithm)
    stats = out[out.record_type == "stats"].iloc[0]
    assert stats.events_in_batch == 400
    assert state.exists and json.loads(state.get[0])["events"] == 400
