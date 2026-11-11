"""Offline, reproducible evaluation of the detection algorithms.

Replays hours of simulated telemetry (virtual clock, seeded) through the *same*
``DetectionEngine`` that runs inside Spark and scores every algorithm against the
injected ground truth:

* precision           share of emitted alerts that fall inside an injected episode on the
                      same device and metric (episode end + grace period)
* recall              share of episodes with at least one matching alert, per anomaly type
* detection delay     first matching alert - episode start
* false positive rate raw (pre-cooldown) detections on normal samples / normal samples
* engine throughput   events per second on one core (pure Python)

Usage::

    python -m benchmarks.detection_evaluation --hours 6 --devices 20 --seeds 1 2 3
"""

from __future__ import annotations

import argparse
import bisect
import json
import platform
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from detection.engine import DetectionEngine
from producer.anomaly_injector import ANOMALY_TYPES, DataQualityConfig
from producer.event_schema import NUMERIC_FIELDS, parse_event_time
from producer.simulator import SimulationConfig, TelemetrySimulator

ALGORITHMS = ("threshold", "zscore", "cusum", "drift")
START = 1_767_225_600.0  # 2026-01-01T00:00:00Z
RESULTS_DIR = Path(__file__).resolve().parent / "results"


@dataclass
class Window:
    start: float
    end: float
    episode_id: str
    anomaly_type: str


def build_index(episodes, grace: float) -> dict[tuple[str, str], tuple[list[float], list[Window]]]:
    """(device, metric) -> windows sorted by start, for O(log n) matching."""
    idx: dict[tuple[str, str], list[Window]] = defaultdict(list)
    for e in episodes:
        idx[(e.device_id, e.metric)].append(Window(e.start, e.end + grace, e.episode_id, e.anomaly_type))
    out = {}
    for key, wins in idx.items():
        wins.sort(key=lambda w: w.start)
        out[key] = ([w.start for w in wins], wins)
    return out


def match(index, device: str, metric: str, ts: float) -> Window | None:
    entry = index.get((device, metric))
    if entry is None:
        return None
    starts, wins = entry
    i = bisect.bisect_right(starts, ts) - 1
    # windows on one (device, metric) never overlap, but check one step back for grace overlap
    for j in (i, i - 1):
        if 0 <= j < len(wins) and wins[j].start <= ts < wins[j].end:
            return wins[j]
    return None


def evaluate_seed(seed: int, hours: float, devices: int, anomaly_rate: float, intensity: float,
                  grace: float, engine: DetectionEngine) -> dict:
    cfg = SimulationConfig(devices=devices, rate=devices, seed=seed, anomaly_rate=anomaly_rate,
                           intensity=intensity, data_quality=DataQualityConfig(enabled=False), run_id=f"s{seed}")
    sim = TelemetrySimulator(cfg, start_time=START)
    events = []
    for m in sim.generate(START, hours * 3600, step_s=5.0):
        e = dict(m.event)
        e["event_time"] = parse_event_time(e["event_time"]).timestamp()
        events.append(e)
    episodes = list(sim.episodes)
    index = build_index(episodes, grace)

    states: dict[str, dict] = {}
    raw_fp = defaultdict(int)
    alerts = defaultdict(list)    # algorithm -> [(ts, device, metric, matched_window | None)]
    normal_samples = 0

    t0 = time.perf_counter()
    for e in events:
        st = states.get(e["device_id"])
        if st is None:
            st = states[e["device_id"]] = engine.new_device_state()
        detections, emitted = engine.process_event(st, e)
        for d in detections:
            if match(index, d.device_id, d.metric, d.detected_at) is None:
                raw_fp[d.algorithm] += 1
        for a in emitted:
            r = a.result
            alerts[r.algorithm].append((r.detected_at, r.device_id, r.metric,
                                        match(index, r.device_id, r.metric, r.detected_at)))
    elapsed = time.perf_counter() - t0

    for e in events:
        for metric in NUMERIC_FIELDS:
            if match(index, e["device_id"], metric, e["event_time"]) is None:
                normal_samples += 1

    per_algorithm = {}
    for alg in ALGORITHMS:
        rows = alerts.get(alg, [])
        tp = [r for r in rows if r[3] is not None]
        first_hit: dict[str, float] = {}
        for ts, _, _, w in tp:
            if w.episode_id not in first_hit or ts < first_hit[w.episode_id]:
                first_hit[w.episode_id] = ts
        by_type = {}
        for name in ANOMALY_TYPES:
            eps = [e for e in episodes if e.anomaly_type == name]
            hits = [e for e in eps if e.episode_id in first_hit]
            delays = [first_hit[e.episode_id] - e.start for e in hits]
            # Detected while the anomaly was still developing, i.e. before the episode ended.
            # Separates "caught the drift" from "noticed the snap back to baseline afterwards".
            during = sum(1 for e in hits if first_hit[e.episode_id] < e.end)
            by_type[name] = {"episodes": len(eps), "detected": len(hits), "detected_during": during,
                             "delays_s": delays}
        per_algorithm[alg] = {
            "alerts": len(rows),
            "true_positive_alerts": len(tp),
            "false_positive_alerts": len(rows) - len(tp),
            "raw_false_detections": raw_fp.get(alg, 0),
            "by_type": by_type,
        }

    return {
        "seed": seed,
        "events": len(events),
        "normal_samples": normal_samples,
        "episodes": len(episodes),
        "device_hours": devices * hours,
        "engine_seconds": elapsed,
        "per_algorithm": per_algorithm,
    }


def aggregate(runs: list[dict]) -> dict:
    summary = {}
    total_normal = sum(r["normal_samples"] for r in runs)
    device_hours = sum(r["device_hours"] for r in runs)
    for alg in ALGORITHMS:
        alerts = sum(r["per_algorithm"][alg]["alerts"] for r in runs)
        tp = sum(r["per_algorithm"][alg]["true_positive_alerts"] for r in runs)
        fp = sum(r["per_algorithm"][alg]["false_positive_alerts"] for r in runs)
        raw_fp = sum(r["per_algorithm"][alg]["raw_false_detections"] for r in runs)
        by_type = {}
        all_eps = all_hits = 0
        all_delays: list[float] = []
        for name, spec in ANOMALY_TYPES.items():
            eps = sum(r["per_algorithm"][alg]["by_type"][name]["episodes"] for r in runs)
            hits = sum(r["per_algorithm"][alg]["by_type"][name]["detected"] for r in runs)
            during = sum(r["per_algorithm"][alg]["by_type"][name]["detected_during"] for r in runs)
            delays = [d for r in runs for d in r["per_algorithm"][alg]["by_type"][name]["delays_s"]]
            all_eps += eps
            all_hits += hits
            all_delays += delays
            by_type[name] = {
                "category": spec["category"],
                "episodes": eps,
                "detected": hits,
                "recall": hits / eps if eps else None,
                "recall_during": during / eps if eps else None,
                "median_delay_s": statistics.median(delays) if delays else None,
                "p90_delay_s": (statistics.quantiles(delays, n=10)[-1] if len(delays) >= 2 else
                                (delays[0] if delays else None)),
            }
        summary[alg] = {
            "alerts": alerts,
            "precision": tp / alerts if alerts else None,
            "recall": all_hits / all_eps if all_eps else None,
            "false_positive_rate": raw_fp / total_normal if total_normal else None,
            "false_alerts_per_device_hour": fp / device_hours,
            "median_delay_s": statistics.median(all_delays) if all_delays else None,
            "by_type": by_type,
        }
    # Ensemble: an episode counts as detected if any algorithm detected it.
    return summary


def ensemble_recall(runs: list[dict]) -> dict:
    out = {}
    for name in ANOMALY_TYPES:
        eps = sum(r["per_algorithm"]["threshold"]["by_type"][name]["episodes"] for r in runs)
        out[name] = {"episodes": eps}
    return out


def to_markdown(result: dict) -> str:
    s = result["summary"]
    lines = [
        "| Detector | Alerts | Precision | Recall (all types) | False-positive rate | False alerts / device-hour | Median delay |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]

    def pct(v):
        return "n/a" if v is None else f"{100 * v:.1f}%"

    for alg in ALGORITHMS:
        a = s[alg]
        delay = "n/a" if a["median_delay_s"] is None else f"{a['median_delay_s']:.0f} s"
        lines.append(f"| {alg} | {a['alerts']} | {pct(a['precision'])} | {pct(a['recall'])} | "
                     f"{a['false_positive_rate'] * 100:.4f}% | {a['false_alerts_per_device_hour']:.3f} | {delay} |")
    lines += ["", "Recall by anomaly type (median detection delay in brackets):", "",
              "| Anomaly type | Category | Episodes | " + " | ".join(ALGORITHMS) + " |",
              "|---|---|---:|" + "---:|" * len(ALGORITHMS)]
    for name, spec in ANOMALY_TYPES.items():
        cells = []
        for alg in ALGORITHMS:
            t = s[alg]["by_type"][name]
            if not t["episodes"]:
                cells.append("n/a")
                continue
            delay = "" if t["median_delay_s"] is None else f" ({t['median_delay_s']:.0f} s)"
            cells.append(f"{100 * t['recall']:.0f}%{delay}")
        eps = s["threshold"]["by_type"][name]["episodes"]
        lines.append(f"| {name} | {spec['category']} | {eps} | " + " | ".join(cells) + " |")
    gradual = [n for n, spec in ANOMALY_TYPES.items() if spec["category"] == "gradual"]
    lines += ["", "Gradual anomalies detected *while still developing* (first alert before the episode ended; "
              "the recall above also credits alerts on the snap back to baseline at the end of an episode):", "",
              "| Anomaly type | " + " | ".join(ALGORITHMS) + " |", "|---|" + "---:|" * len(ALGORITHMS)]
    for name in gradual:
        cells = [f"{100 * s[a]['by_type'][name]['recall_during']:.0f}%" for a in ALGORITHMS]
        lines.append(f"| {name} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hours", type=float, default=6.0)
    parser.add_argument("--devices", type=int, default=20)
    parser.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3])
    parser.add_argument("--anomaly-rate", type=float, default=2.0, help="episodes per device-hour")
    parser.add_argument("--intensity", type=float, default=1.0)
    parser.add_argument("--grace", type=float, default=180.0, help="seconds after an episode still credited")
    parser.add_argument("--out", type=Path, default=RESULTS_DIR / "detection_evaluation.json")
    args = parser.parse_args()

    engine = DetectionEngine.from_yaml()
    runs = []
    for seed in args.seeds:
        r = evaluate_seed(seed, args.hours, args.devices, args.anomaly_rate, args.intensity, args.grace, engine)
        print(f"seed {seed}: {r['events']:,} events, {r['episodes']} episodes, "
              f"engine {r['events'] / r['engine_seconds']:,.0f} events/s")
        runs.append(r)

    result = {
        "config": vars(args) | {"out": str(args.out)},
        "environment": {"python": platform.python_version(), "machine": platform.machine(),
                        "processor": platform.processor()},
        "engine_events_per_second": sum(r["events"] for r in runs) / sum(r["engine_seconds"] for r in runs),
        "summary": aggregate(runs),
        "runs": [{k: v for k, v in r.items() if k != "per_algorithm"} for r in runs],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2))
    md = to_markdown(result)
    args.out.with_suffix(".md").write_text(md + "\n", encoding="utf-8")
    print()
    print(md)
    print(f"\nengine throughput: {result['engine_events_per_second']:,.0f} events/s (single core, all detectors)")
    print(f"written: {args.out}")


if __name__ == "__main__":
    main()
