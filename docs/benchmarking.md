# Benchmarking

Three harnesses, all under `benchmarks/`, writing JSON to `benchmarks/results/`.
`python -m benchmarks.generate_reports` renders them into
[`benchmarks/results/REPORT.md`](../benchmarks/results/REPORT.md) and the PNG charts.

| Harness | Needs the stack | Reproducible | Measures |
|---|:---:|:---:|---|
| `detection_evaluation.py` | no | yes (seeded) | precision, recall, delay, false-positive rate per detector and anomaly type |
| `load_test.py` | yes | per machine | throughput, latency, batch duration, CPU/memory, sustainability per rate |
| `recovery_test.py` | yes | per machine | recovery time, checkpoint resume, data completeness, duplicate alerts |

All numbers are measurements on the machine described, not guarantees.

## 1. Detection quality (offline)

```bash
python -m benchmarks.detection_evaluation --hours 6 --devices 20 --seeds 1 2 3
```

The simulator runs on a virtual clock (6 hours of telemetry in about 40 s), so the same seeds
always produce the same events and the same 508 anomaly episodes. Events go through the
**same `DetectionEngine` that runs inside Spark**, with the same configuration and cooldowns.

Scoring:

* An alert is a **true positive** when it falls inside an injected episode on the *same
  device and metric*, between the episode start and its end + 180 s grace.
* **Recall** is the share of episodes with at least one matching alert. Because the grace
  window also credits the abrupt return to baseline at the end of a long drift (the simulated
  "maintenance"), the report adds **recall while the anomaly was active**: first alert before
  the episode ended. Without it, z-score appears to catch 94% of temperature drifts. It
  actually catches 0% while they develop and only notices the snap back.
* **False-positive rate** counts raw detections (before cooldown) on normal samples, divided
  by normal samples. **False alerts per device-hour** counts emitted alerts outside any episode.

Headline (3 seeds × 6 h × 20 devices, 1.3 M events):

| | Threshold | Z-score | CUSUM | Drift |
|---|---:|---:|---:|---:|
| Precision | 100% | 98.7% | 97.5% | 94.1% |
| False alerts / device-hour | 0.000 | 0.017 | 0.072 | 0.072 |
| Sudden faults caught | 11-19% | 98-100% | 92-97% (spikes 0%) | 0-45% |
| Gradual drifts caught while developing | 0-4% | 0-1% | 99-100% | 94-100% |
| Median delay | 0 s | 0 s | 4 s | 329 s |

### CUSUM tuning

| warm-up | h | Precision | False alerts / device-hour | Drift delay (median) |
|---:|---:|---:|---:|---:|
| 120 | 10 | 78.1% | 0.77 | 110 s |
| 300 | 10 | 87.0% | 0.43 | 110 s |
| **300** | **12** | **96.1%** | **0.12** | **118 s** |
| 600 | 12 | 96.6% | 0.08 | 124 s |

(2 seeds × 3 h sweep.) The short warm-up was the main cause of false alarms: the error in
the estimated baseline effectively shrinks the allowance `k`. 300/12 was kept as the best
trade-off between false alerts and warm-up time.

## 2. Live load test

```bash
python -m benchmarks.load_test --rates 100 500 1000 2000 4000 8000 --devices 50
```

For each rate the script reconfigures the simulator through `POST /simulation/start`, waits
for a warm-up, then measures over a window using only what the pipeline recorded
(`pipeline_metrics`, `alerts`) plus `docker stats` samples. A rate is **sustainable** when the
validated throughput is at least 95% of what the producer sent *and* detection lag is not
growing across the window.

Machine: Windows 10 laptop, Docker Desktop (WSL2) with 4 CPUs / 7.7 GiB **shared with every
other container on the host**. Spark: 1 worker, 2 cores, 1 GB executor; 2 shuffle partitions;
HDFS-backed state store. Data-quality faults on.

| Target events/s | Validated events/s | Kafka → validated | Event → detection (mean / p95) | Batch: validator / aggregator / detector | Spark worker CPU | Sustainable |
|---:|---:|---:|---:|---:|---:|:---:|
| 100 | 101 | 2.5 s | 7.0 s / 8.1 s | 1.6 / 3.9 / 3.1 s | 57% | yes |
| 500 | 501 | 2.6 s | 7.0 s / 7.9 s | 1.7 / 4.2 / 3.1 s | 40% | yes |
| 1,000 | 1,002 | 2.7 s | 7.2 s / 9.5 s | 1.7 / 4.4 / 3.1 s | 47% | yes |
| 2,000 | 2,004 | 2.8 s | 8.5 s / 10.7 s | 1.9 / 4.6 / 3.1 s | 94% | yes |
| 4,000 | 4,019 | 3.6 s | 9.5 s / 12.0 s | 2.6 / 5.2 / 4.2 s | 149% | yes |
| 8,000 | 5,533 | 8.0 s | 18.4 s / 24.5 s | 7.6 / 7.8 / 9.9 s | 250% | **no** |

Reading it:

* Up to 4,000 events/s, latency is almost flat. It is set by micro-batch scheduling (2 s
  validator trigger, 5 s detector trigger, a Kafka hop in between), not by the work itself.
* Between 4,000 and 8,000 events/s the two Spark cores saturate (worker CPU 250% = both
  cores plus the Python workers). The ceiling for this configuration is about 5,500 events/s.
* The detection engine alone runs at about 32,000 events/s per core, so detection is not the
  bottleneck. Parsing, de-duplication state and the extra Kafka hop are.

### Optimization history (100 events/s, same machine)

| Change | Micro-batch duration |
|---|---|
| First version: 6 shuffle partitions, RocksDB, per-partition PostgreSQL writes, separate Kafka jobs | 11-14 s |
| Bounded outputs (windows, alerts, rejects) collected and written from the driver in one transaction; one Kafka job per batch with per-row topic routing | 5-7 s |
| 2 shuffle partitions (= executor cores), in-memory HDFS-backed state store | 1.6-4 s |

The diagnosis came from Spark's own numbers: `durationMs` per trigger (logged by the query
listener) showed `addBatch` dominating, and the REST stage list showed about 1.4 s of fixed
overhead per stage with only 0.2 s of executor work per task. The fix was fewer stages, not
faster code.

## 3. Failure recovery

```bash
python -m benchmarks.recovery_test --outage 30 --settle 180
```

| Failure | Down for | Recovered after restart | Resumed from checkpoint | Windows complete | Event coverage | Duplicate alert keys |
|---|---:|---:|:---:|---:|---:|---:|
| Spark driver SIGKILL | 30 s | 30 s | yes | 180/180 | 99.5% | 0 |
| Kafka broker stop/start | 30 s | 23 s | yes | 160/160 | 99.5% | 0 |
| PostgreSQL stop/start | 30 s | 11 s | yes | 160/160 | 99.5% | 0 |

* **Recovered after restart:** time from the component coming back until every streaming
  query committed a new batch.
* **Resumed from checkpoint:** batch ids continue from the last committed id; nothing
  restarted at zero or reprocessed from the beginning.
* **Event coverage:** events counted in every 1-minute window spanning the failure, divided
  by what the simulator sent. The 0.5% gap is identical in normal operation: injected
  duplicates (removed), malformed records (dead-lettered) and events deliberately delayed
  beyond the 2-minute watermark (dropped as too late). **No data was lost** in any scenario.
* **Duplicate alert keys:** zero by construction (deterministic ids + `ON CONFLICT`). The
  test also looks for near-duplicates (same device, metric and algorithm less than 1 s apart).
  Every one it found was a severity escalation on a different event, which the cooldown
  deliberately lets through.

What the scenarios exercise: Spark recovering offsets and state from its checkpoint; the
producer buffering through a broker outage (librdkafka retries, idempotent producer); and a
failing `foreachBatch` killing the query, after which the container's `restart: on-failure`
brings the driver back to replay the failed batch idempotently.

## Bugs the benchmarks found

* **CardinalityViolation crash loop (load test).** Reconfiguring the fleet re-assigned some
  devices to other plants. Late events from the old configuration then produced two
  `(device, window)` rows with different `plant_id`s in one batch, Postgres rejected the
  upsert, and the driver crash-looped on the replayed batch. Fix: group windows by device
  only (plant is an attribute) and de-duplicate by upsert key before writing.
* **Live evaluation under-counting (dashboard).** Alert and episode start times are the same
  instant but travel different float paths (Spark `timestamp_seconds` vs Python
  `fromtimestamp`), so the `>=` comparison failed by microseconds. Fix: 1 s tolerance.
* **Misleading recall (offline evaluation).** See "recall while the anomaly was active" above.
