"""Failure-recovery test against the running Docker Compose stack.

Three scenarios, one after another:

* ``spark_kill``       SIGKILL the Spark driver, keep it down for ``--outage`` seconds, start it
* ``kafka_restart``    restart the broker (the producer buffers meanwhile)
* ``postgres_restart`` restart the database (foreachBatch fails, the driver restarts itself)

For each scenario it measures, from data the pipeline itself persisted:

* recovery time      until every streaming query has committed a new micro-batch
* checkpoint resume  batch ids continue from before the failure (not restarted at 0)
* completeness       events counted in the 1-minute windows that span the failure, vs.
                     what the simulator sent (rate / devices is known exactly)
* duplicate alerts   alerts sharing (device, metric, algorithm, triggering event) - must be 0
                     by construction - and near-identical alerts within 1 s

Usage::

    python -m benchmarks.recovery_test --outage 30 --settle 180
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import psycopg.types.numeric
import requests

OUT = Path(__file__).resolve().parent / "results" / "recovery_test.json"
QUERIES = ("validator", "aggregator", "detector")


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True, timeout=180)


def latest_batches(conn) -> dict[str, tuple[int, datetime]]:
    rows = conn.execute(
        "SELECT DISTINCT ON (query_name) query_name, batch_id, recorded_at FROM pipeline_metrics "
        "ORDER BY query_name, recorded_at DESC").fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def wait_recovered(dsn: str, since: datetime, timeout: float = 600) -> tuple[float | None, dict]:
    """Seconds until every query has committed a batch after `since`."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
                latest = latest_batches(conn)
            if all(q in latest and latest[q][1] > since for q in QUERIES):
                return (max(latest[q][1] for q in QUERIES) - since).total_seconds(), latest
        except psycopg.OperationalError:
            pass
        time.sleep(3)
    return None, {}


def completeness(conn, start: datetime, end: datetime, per_window: float, devices: int) -> dict:
    """Event coverage of the 1-minute windows fully inside [start, end] for simulator devices."""
    rows = conn.execute(
        """SELECT device_id, window_start, event_count FROM telemetry_metrics
            WHERE device_id LIKE 'sensor_%%' AND window_start >= %s AND window_end <= %s""",
        (start, end)).fetchall()
    # Windows are aligned to 15 s boundaries: count the aligned starts whose window fits.
    first = int(-(-start.timestamp() // 15) * 15)
    n_windows = len(range(first, int(end.timestamp()) - 60 + 1, 15))
    expected_rows = n_windows * devices
    counted = sum(min(r[2], per_window) for r in rows)
    return {
        "windows_expected": expected_rows,
        "windows_found": len(rows),
        "event_coverage": counted / (expected_rows * per_window) if expected_rows else None,
    }


def duplicate_alerts(conn, start: datetime, end: datetime) -> dict:
    exact = conn.execute(
        """SELECT count(*) FROM (SELECT device_id, metric_name, algorithm, event_id FROM alerts
            WHERE detected_at BETWEEN %s AND %s GROUP BY 1,2,3,4 HAVING count(*) > 1) d""", (start, end)).fetchone()[0]
    near = conn.execute(
        """SELECT count(*) FROM alerts a JOIN alerts b
             ON a.device_id = b.device_id AND a.metric_name = b.metric_name AND a.algorithm = b.algorithm
            AND a.alert_id < b.alert_id AND abs(extract(epoch FROM a.detected_at - b.detected_at)) < 1
            WHERE a.detected_at BETWEEN %s AND %s""", (start, end)).fetchone()[0]
    total = conn.execute("SELECT count(*) FROM alerts WHERE detected_at BETWEEN %s AND %s", (start, end)).fetchone()[0]
    return {"alerts": total, "duplicate_keys": exact, "near_duplicates_within_1s": near}


SCENARIOS = {
    "spark_kill": lambda outage: (compose("kill", "stream-processor"), time.sleep(outage), compose("start", "stream-processor")),
    "kafka_restart": lambda outage: (compose("stop", "kafka"), time.sleep(outage), compose("start", "kafka")),
    "postgres_restart": lambda outage: (compose("stop", "postgres"), time.sleep(outage), compose("start", "postgres")),
}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--dsn", default="host=localhost port=5432 dbname=streampulse user=streampulse password=streampulse")
    p.add_argument("--scenarios", nargs="+", default=list(SCENARIOS))
    p.add_argument("--outage", type=int, default=30, help="seconds the component stays down")
    p.add_argument("--settle", type=int, default=180, help="seconds after recovery before scoring (> watermark)")
    p.add_argument("--out", type=Path, default=OUT)
    p.add_argument("--rescore", action="store_true", help="recompute scores for an existing results file")
    args = p.parse_args()
    if args.rescore:
        return rescore(args)

    sim = requests.get(f"{args.api}/simulation/status", timeout=10).json()
    per_window = 60.0 * sim["rate"] / sim["devices"]
    results = []
    for name in args.scenarios:
        with psycopg.connect(args.dsn, autocommit=True) as conn:
            before = latest_batches(conn)
        start = datetime.now(timezone.utc)
        print(f"--- {name}: failing for {args.outage}s", flush=True)
        SCENARIOS[name](args.outage)
        restored = datetime.now(timezone.utc)
        recovery_s, after = wait_recovered(args.dsn, restored)
        print(f"    recovered in {recovery_s}s after restart; settling {args.settle}s", flush=True)
        time.sleep(args.settle)
        with psycopg.connect(args.dsn, autocommit=True) as conn:
            conn.adapters.register_loader("numeric", psycopg.types.numeric.FloatLoader)
            span_start = start - timedelta(seconds=60)
            span_end = restored + timedelta(seconds=max(60, recovery_s or 0) + 30)
            res = {
                "scenario": name,
                "outage_s": args.outage,
                "recovery_after_restart_s": recovery_s,
                "checkpoint_resume": {q: {"before": before.get(q, (None,))[0], "after": after.get(q, (None,))[0]}
                                      for q in QUERIES},
                "resumed_from_checkpoint": all(after.get(q, (-1,))[0] > before.get(q, (10**9,))[0] for q in QUERIES),
                "completeness": completeness(conn, span_start, span_end, per_window, sim["devices"]),
                "duplicates": duplicate_alerts(conn, span_start, span_end),
                "failure_window": [start.isoformat(), restored.isoformat()],
            }
        results.append(res)
        print(json.dumps(res, default=str), flush=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"measured_at": datetime.now(timezone.utc).isoformat(),
                                    "simulation": {k: sim[k] for k in ("devices", "rate", "seed")},
                                    "results": results}, indent=2, default=str))
    print("written", args.out)


def rescore(args) -> None:
    data = json.loads(args.out.read_text())
    per_window = 60.0 * data["simulation"]["rate"] / data["simulation"]["devices"]
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        conn.adapters.register_loader("numeric", psycopg.types.numeric.FloatLoader)
        for res in data["results"]:
            start, restored = (datetime.fromisoformat(t) for t in res["failure_window"])
            span_start = start - timedelta(seconds=60)
            span_end = restored + timedelta(seconds=max(60, res["recovery_after_restart_s"] or 0) + 30)
            res["completeness"] = completeness(conn, span_start, span_end, per_window, data["simulation"]["devices"])
            res["duplicates"] = duplicate_alerts(conn, span_start, span_end)
            print(res["scenario"], res["completeness"], res["duplicates"])
    args.out.write_text(json.dumps(data, indent=2, default=str))


if __name__ == "__main__":
    main()
