# Data flow

## 1. Event contract

```json
{
  "event_id": "evt-4301777-sensor_001-00000042",
  "device_id": "sensor_001",
  "plant_id": "plant_01",
  "event_time": "2026-11-10T09:30:15.250Z",
  "temperature": 65.42, "vibration": 1.712, "pressure": 99.31,
  "rpm": 1391.6, "power_consumption": 5.204,
  "status": "running",
  "schema_version": 1
}
```

* `event_id` = `evt-<run>-<device>-<sequence>`: unique per producer run, so restarts never
  reuse ids, which matters because alert ids are derived from them.
* Kafka key = `device_id`: all events of one device land in one partition, in order.
* Missing measurements are `null` and are allowed. They feed the missing-measurement rate and
  detectors skip them.

## 2. Simulator

`producer/simulator.py`. Each device has its own baseline per metric
(`producer/sensor.py`); RPM is coupled to power draw and heat. Everything is seeded:
the same seed produces the same fleet, the same noise and the same anomaly schedule.

Anomalies (`producer/anomaly_injector.py`) are a Poisson process per device, one episode at
a time, recorded as ground truth in `injected_anomalies`:

| Type | Shape | Magnitude (x intensity) | Duration |
|---|---|---|---|
| temperature_spike | single sample | +16 to +28 °C | 1 sample |
| vibration_burst | step | +2.2 to +4.0 mm/s | 3-8 s |
| pressure_drop | step | −8 to −14 kPa | 20-60 s |
| power_surge | step | +2.0 to +3.5 kW | 10-30 s |
| temperature_drift | ramp, then hold | +7 to +13 °C | 15-30 min ramp + 10-20 min hold |
| vibration_degradation | ramp + growing noise (x2.5) | +1.0 to +2.0 mm/s | 15-30 min ramp + 10-20 min hold |

Data-quality faults (on by default): 1% missing measurement, 0.5% duplicate, 1% delayed by
5-200 s (some beyond the 2-minute watermark on purpose), 0.2% malformed (truncated JSON,
missing device, string measurement, negative RPM, impossible timestamp).

## 3. Validator (`raw-telemetry` → `validated-telemetry` | `invalid-telemetry`)

Payloads are parsed with every field as STRING plus a `_corrupt_record` column, so Spark,
not the JSON reader, decides what is wrong. Each record gets the first reason it fails:

1. `malformed_json` 2. `missing_required_field` 3. `invalid_event_time`
4. `non_numeric_measurement` 5. `out_of_physical_range` 6. `no_measurements`

Spark 4 runs ANSI SQL by default, where `CAST('abc' AS DOUBLE)` raises an error. Validation
uses `try_cast` only. The same rules exist in pure Python (`producer/event_schema.validate_event`),
and `tests/test_transformations.py` asserts both implementations agree on every fixture.

Valid records are re-serialized with typed values plus `ingest_time` (Kafka timestamp) and
`validated_at`. Rejected records go to `invalid-telemetry` (7-day retention) and
`dead_letter_events`, keyed by `topic-partition-offset` so replays are idempotent.

## 4. Aggregator (`validated-telemetry` → `telemetry_metrics`)

```
withWatermark(event_time, 2 minutes)
  .dropDuplicatesWithinWatermark(event_id)
  .groupBy(window(event_time, 1 minute, 15 seconds), device_id, plant_id)
```

Output mode *update*. Each batch upserts only the windows that changed, keyed on
`(device_id, window_start, window_end)`. Late events refine their window until the
watermark passes it, and events older than the watermark are dropped. Per window: mean, min,
max and standard deviation of temperature, mean/max/sd of vibration, mean/min pressure, mean
RPM and power, event count and missing-measurement rate.

## 5. Detector (`validated-telemetry` → `alerts` + `anomaly-events`)

Same watermark + dedup, then `groupBy(device_id).applyInPandasWithState(...)`. Within a
micro-batch each device's events are sorted by event time and fed through every detector on
every metric. Late events that are still inside the watermark are processed in arrival
order, which is a deliberate trade-off: re-ordering across batches would need buffering and
add latency.

Output per batch: alert rows, plus one stats row per device (events processed, mean lag),
which becomes the detector's line in `pipeline_metrics`.

## 6. Persistence

| Table | Key | Written by |
|---|---|---|
| `telemetry_metrics` | `(device_id, window_start, window_end)` upsert | aggregator |
| `devices` | `device_id` upsert | producer (registry), aggregator (`last_seen_at`) |
| `alerts` | `alert_id` = sha1(device, metric, algorithm, event_id), insert-or-ignore | detector |
| `dead_letter_events` | `topic-partition-offset`, insert-or-ignore | validator |
| `pipeline_metrics` | `(query_name, batch_id)` upsert | every query + `StreamingQueryListener` |
| `injected_anomalies` | `episode_id` | producer |
| `simulation_control` | single row | API (control), producer (heartbeat) |

TimescaleDB is optional: `database/migrations/001_optional_timescaledb.sql` turns
`telemetry_metrics` into a hypertable with a retention policy.
