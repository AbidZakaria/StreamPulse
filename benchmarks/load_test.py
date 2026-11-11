"""Live load test against the running Docker Compose stack.

For each target rate the script reconfigures the simulator through the API, lets the
pipeline settle, then measures over a fixed window, using only what the pipeline itself
recorded in PostgreSQL:

* throughput             validated + rejected events per second (validator query)
* producer rate          what the simulator actually sent
* processing latency     Kafka ingest -> validated (validator batches)
* event -> detection     sensor event time -> detector processed it (detector batches)
* alert persistence      detected_at -> persisted_at for alerts written in the window
* batch duration         per query, mean and max
* sustainable            throughput >= 95% of the producer rate AND detection lag not
                         growing across the window (no backlog building up)
* CPU / memory           ``docker stats`` samples for the Spark, Kafka and Postgres containers

These are measurements on *this* machine, not guarantees.

Usage::

    python -m benchmarks.load_test --rates 100 500 1000 2000 --devices 50 --warmup 60 --measure 120
"""

from __future__ import annotations

import argparse
import json
import platform
import re
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import psycopg.types.numeric
import requests

RESULTS = Path(__file__).resolve().parent / "results" / "load_test.json"
CONTAINERS = ("streampulse-spark-worker-1", "streampulse-stream-processor-1", "streampulse-kafka-1",
              "streampulse-postgres-1", "streampulse-producer-1")


def docker_stats() -> dict[str, dict]:
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *CONTAINERS],
            capture_output=True, text=True, timeout=30, check=True,
        ).stdout
    except (subprocess.SubprocessError, FileNotFoundError):
        return {}
    stats = {}
    for line in out.splitlines():
        row = json.loads(line)
        m = re.match(r"\s*([\d.]+)\s*([KMGT]i?B|B)", row["MemUsage"])
        if not m:
            continue
        factor = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "TiB": 2**20,
                  "kB": 1 / 1024, "KB": 1 / 1024, "MB": 1, "GB": 1024}.get(m.group(2), 1)
        try:
            cpu = float(row["CPUPerc"].rstrip("%") or 0)
        except ValueError:
            cpu = 0.0
        stats[row["Name"]] = {"cpu_pct": cpu, "mem_mib": float(m.group(1)) * factor}
    return stats


def measure(conn, t0: datetime, t1: datetime) -> dict:
    q = conn.execute
    v = q("""
        SELECT coalesce(sum(events_processed + events_rejected), 0) AS events,
               avg(processing_latency_ms) AS proc_ms, avg(batch_duration_ms) AS batch_ms,
               max(batch_duration_ms) AS batch_max_ms, count(*) AS batches
          FROM pipeline_metrics WHERE query_name = 'validator' AND recorded_at BETWEEN %s AND %s""", (t0, t1)).fetchone()
    d = q("""
        SELECT avg(end_to_end_latency_ms) AS lag_ms, avg(batch_duration_ms) AS batch_ms,
               max(batch_duration_ms) AS batch_max_ms, coalesce(sum(events_processed), 0) AS events
          FROM pipeline_metrics WHERE query_name = 'detector' AND recorded_at BETWEEN %s AND %s""", (t0, t1)).fetchone()
    a = q("""
        SELECT avg(batch_duration_ms) AS batch_ms, max(batch_duration_ms) AS batch_max_ms
          FROM pipeline_metrics WHERE query_name = 'aggregator' AND recorded_at BETWEEN %s AND %s""", (t0, t1)).fetchone()
    lags = [r[0] for r in q("""
        SELECT end_to_end_latency_ms FROM pipeline_metrics
         WHERE query_name = 'detector' AND recorded_at BETWEEN %s AND %s AND end_to_end_latency_ms IS NOT NULL
         ORDER BY recorded_at""", (t0, t1)).fetchall()]
    alert = q("""
        SELECT avg(extract(epoch FROM persisted_at - detected_at)) * 1000 AS ms, count(*) AS n
          FROM alerts WHERE persisted_at BETWEEN %s AND %s""", (t0, t1)).fetchone()
    secs = (t1 - t0).total_seconds()
    third = max(1, len(lags) // 3)
    early = statistics.mean(lags[:third]) if lags else None
    late = statistics.mean(lags[-third:]) if lags else None
    return {
        "window_s": secs,
        "throughput_eps": v[0] / secs,
        "detector_eps": d[3] / secs,
        "processing_latency_ms": v[1],
        "event_to_detection_ms": d[0],
        "event_to_detection_p95_ms": (statistics.quantiles(lags, n=20)[-1] if len(lags) >= 2 else None),
        "lag_first_third_ms": early,
        "lag_last_third_ms": late,
        "alert_persist_latency_ms": alert[0],
        "alerts": alert[1],
        "batch_ms": {"validator": v[2], "aggregator": a[0], "detector": d[1]},
        "batch_max_ms": {"validator": v[3], "aggregator": a[1], "detector": d[2]},
        "validator_batches": v[4],
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--dsn", default="host=localhost port=5432 dbname=streampulse user=streampulse password=streampulse")
    p.add_argument("--rates", type=float, nargs="+", default=[100, 500, 1000, 2000])
    p.add_argument("--devices", type=int, default=50)
    p.add_argument("--warmup", type=int, default=60)
    p.add_argument("--measure", type=int, default=120)
    p.add_argument("--restore-rate", type=float, default=100)
    p.add_argument("--out", type=Path, default=RESULTS)
    args = p.parse_args()

    conn = psycopg.connect(args.dsn, autocommit=True)
    # numeric (e.g. SUM over bigint) -> float, so arithmetic with Python floats just works
    conn.adapters.register_loader("numeric", psycopg.types.numeric.FloatLoader)
    results = []
    try:
        for rate in args.rates:
            print(f"--- rate {rate:.0f} ev/s: configuring", flush=True)
            requests.post(f"{args.api}/simulation/start",
                          json={"rate": rate, "devices": args.devices, "data_quality": True}, timeout=10).raise_for_status()
            time.sleep(args.warmup)
            t0 = datetime.now(timezone.utc)
            samples, producer_rates = [], []
            end = time.time() + args.measure
            while time.time() < end:
                samples.append(docker_stats())
                producer_rates.append(requests.get(f"{args.api}/simulation/status", timeout=10).json()["producer_rate"])
                time.sleep(max(0.0, min(10.0, end - time.time())))
            t1 = datetime.now(timezone.utc)
            m = measure(conn, t0, t1)
            m["target_rate"] = rate
            m["producer_rate"] = statistics.mean([r for r in producer_rates if r]) if any(producer_rates) else None
            resources = {}
            for name in CONTAINERS:
                rows = [s[name] for s in samples if name in s]
                if rows:
                    resources[name.replace("streampulse-", "").removesuffix("-1")] = {
                        "cpu_pct_mean": statistics.mean(r["cpu_pct"] for r in rows),
                        "cpu_pct_max": max(r["cpu_pct"] for r in rows),
                        "mem_mib_max": max(r["mem_mib"] for r in rows),
                    }
            m["resources"] = resources
            growing = (m["lag_first_third_ms"] and m["lag_last_third_ms"]
                       and m["lag_last_third_ms"] > 1.5 * m["lag_first_third_ms"] + 2000)
            keeps_up = m["producer_rate"] and m["throughput_eps"] >= 0.95 * m["producer_rate"]
            m["sustainable"] = bool(keeps_up and not growing)
            results.append(m)
            print(json.dumps({k: m[k] for k in ("target_rate", "producer_rate", "throughput_eps",
                                                 "event_to_detection_ms", "sustainable")}, default=str), flush=True)
    finally:
        requests.post(f"{args.api}/simulation/start",
                      json={"rate": args.restore_rate, "devices": 20}, timeout=10)
        conn.close()

    payload = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "config": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items() if k != "dsn"},
        "host": {"os": platform.platform(), "machine": platform.machine(),
                 "docker_cpus": _docker_info("{{.NCPU}}"), "docker_mem": _docker_info("{{.MemTotal}}")},
        "results": results,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2, default=str))
    print(f"written {args.out}")


def _docker_info(fmt: str) -> str | None:
    try:
        return subprocess.run(["docker", "info", "--format", fmt], capture_output=True, text=True,
                              timeout=20).stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        return None


if __name__ == "__main__":
    main()
