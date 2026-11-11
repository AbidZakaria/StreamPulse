"""Render benchmark JSON into PNG charts and a markdown report.

Reads ``benchmarks/results/detection_evaluation.json`` and, when present,
``benchmarks/results/load_test.json``; writes ``benchmarks/results/*.png`` and
``benchmarks/results/REPORT.md``.

    python -m benchmarks.generate_reports
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from benchmarks.detection_evaluation import ALGORITHMS, to_markdown  # noqa: E402
from producer.anomaly_injector import ANOMALY_TYPES  # noqa: E402

RESULTS = Path(__file__).resolve().parent / "results"
INK = "#1e2326"
MUTED = "#485158"
RULE = "#c3cbc7"
COLORS = {"threshold": "#5d676e", "zscore": "#3a3d9c", "cusum": "#8a5a2b", "drift": "#1d7470"}
LABEL = {"threshold": "Threshold", "zscore": "Z-score", "cusum": "CUSUM", "drift": "Drift"}

plt.rcParams.update({
    "font.size": 10, "axes.edgecolor": RULE, "axes.labelcolor": MUTED, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.titleweight": "bold", "axes.titlesize": 11, "axes.titlecolor": INK,
    "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 140, "savefig.bbox": "tight",
    "axes.grid": True, "grid.color": RULE, "grid.linewidth": 0.6, "axes.axisbelow": True,
})


def detection_charts(data: dict) -> list[str]:
    s = data["summary"]
    types = list(ANOMALY_TYPES)
    out = []

    # recall heatmap
    fig, ax = plt.subplots(figsize=(8.4, 3.6))
    grid = [[(s[a]["by_type"][t]["recall_during"] or 0.0) for t in types] for a in ALGORITHMS]
    im = ax.imshow(grid, cmap="Blues", vmin=0, vmax=1, aspect="auto")
    ax.set_xticks(range(len(types)), [t.replace("_", "\n") for t in types], fontsize=9)
    ax.set_yticks(range(len(ALGORITHMS)), [LABEL[a] for a in ALGORITHMS])
    ax.grid(False)
    for i in range(len(ALGORITHMS)):
        for j in range(len(types)):
            v = grid[i][j]
            ax.text(j, i, f"{v * 100:.0f}%", ha="center", va="center", fontsize=9,
                    color="white" if v > 0.55 else INK)
    ax.set_title("Recall while the anomaly was active (first alert before the episode ended)")
    fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
    path = RESULTS / "detection_recall.png"
    fig.savefig(path)
    plt.close(fig)
    out.append(path.name)

    # precision / false alerts
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 3.2))
    names = [LABEL[a] for a in ALGORITHMS]
    a1.bar(names, [100 * (s[a]["precision"] or 0) for a in ALGORITHMS], color=[COLORS[a] for a in ALGORITHMS])
    a1.set_ylim(0, 105)
    a1.set_title("Precision of emitted alerts (%)")
    a2.bar(names, [s[a]["false_alerts_per_device_hour"] for a in ALGORITHMS], color=[COLORS[a] for a in ALGORITHMS])
    a2.set_title("False alerts per device-hour")
    for ax in (a1, a2):
        for p in ax.patches:
            ax.annotate(f"{p.get_height():.2f}" if ax is a2 else f"{p.get_height():.0f}",
                        (p.get_x() + p.get_width() / 2, p.get_height()), ha="center", va="bottom", fontsize=9, color=INK)
    path = RESULTS / "detection_precision.png"
    fig.savefig(path)
    plt.close(fig)
    out.append(path.name)

    # detection delay
    fig, ax = plt.subplots(figsize=(8.4, 3.4))
    width = 0.2
    for k, a in enumerate(ALGORITHMS):
        vals = [s[a]["by_type"][t]["median_delay_s"] for t in types]
        xs = [i + (k - 1.5) * width for i in range(len(types))]
        ax.bar([x for x, v in zip(xs, vals, strict=False) if v is not None], [max(v, 0.5) for v in vals if v is not None],
               width, color=COLORS[a], label=LABEL[a])
    ax.set_yscale("log")
    ax.set_xticks(range(len(types)), [t.replace("_", "\n") for t in types], fontsize=9)
    ax.set_ylabel("median delay (s, log)")
    ax.set_title("Median detection delay by anomaly type (0 s plotted as 0.5 s)")
    ax.legend(ncols=4, frameon=False, fontsize=9, loc="upper left")
    path = RESULTS / "detection_delay.png"
    fig.savefig(path)
    plt.close(fig)
    out.append(path.name)
    return out


def load_charts(data: dict) -> list[str]:
    rows = sorted(data["results"], key=lambda r: r["target_rate"])
    if not rows:
        return []
    out = []
    rates = [r["target_rate"] for r in rows]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.6))
    a1.plot(rates, rates, color=RULE, linestyle="--", label="target")
    a1.plot(rates, [r["producer_rate"] or 0 for r in rows], marker="o", color=MUTED, label="producer sent")
    a1.plot(rates, [r["throughput_eps"] for r in rows], marker="o", color="#3a3d9c", label="pipeline validated")
    for r in rows:
        if not r["sustainable"]:
            a1.annotate("backlog", (r["target_rate"], r["throughput_eps"]), textcoords="offset points",
                        xytext=(0, -14), ha="center", color="#c0291f", fontsize=8)
    a1.set_xlabel("target events / s")
    a1.set_ylabel("events / s")
    a1.set_title("Throughput")
    a1.legend(frameon=False, fontsize=8)
    a2.plot(rates, [(r["processing_latency_ms"] or 0) / 1000 for r in rows], marker="o", color="#3a3d9c",
            label="Kafka -> validated")
    a2.plot(rates, [(r["event_to_detection_ms"] or 0) / 1000 for r in rows], marker="o", color="#8a5a2b",
            label="event -> detection (mean)")
    a2.plot(rates, [(r["event_to_detection_p95_ms"] or 0) / 1000 for r in rows], marker="o", linestyle=":",
            color="#8a5a2b", label="event -> detection (p95 batch)")
    a2.set_xlabel("target events / s")
    a2.set_ylabel("seconds")
    a2.set_title("Latency")
    a2.legend(frameon=False, fontsize=8)
    path = RESULTS / "load_throughput_latency.png"
    fig.savefig(path)
    plt.close(fig)
    out.append(path.name)

    fig, ax = plt.subplots(figsize=(8.4, 3.2))
    names = sorted({n for r in rows for n in r["resources"]})
    width = 0.8 / max(len(names), 1)
    palette = ["#3a3d9c", "#1d7470", "#8a5a2b", "#7a3b69", "#5f6a1f"]
    for k, n in enumerate(names):
        ax.bar([i + (k - (len(names) - 1) / 2) * width for i in range(len(rows))],
               [r["resources"].get(n, {}).get("cpu_pct_mean", 0) for r in rows], width, label=n, color=palette[k % 5])
    ax.set_xticks(range(len(rows)), [f"{r:.0f}" for r in rates])
    ax.set_xlabel("target events / s")
    ax.set_ylabel("mean CPU % (100 = 1 core)")
    ax.set_title("Container CPU during the measurement window")
    ax.legend(frameon=False, fontsize=8, ncols=3)
    path = RESULTS / "load_cpu.png"
    fig.savefig(path)
    plt.close(fig)
    out.append(path.name)
    return out


def load_table(data: dict) -> str:
    lines = ["| Target ev/s | Producer ev/s | Validated ev/s | Kafka→validated | Event→detection (mean / p95) | "
             "Batch validator / aggregator / detector | Spark worker CPU | Sustainable |",
             "|---:|---:|---:|---:|---:|---:|---:|:---:|"]

    def s(ms):
        return "–" if ms is None else f"{ms / 1000:.1f} s"

    for r in sorted(data["results"], key=lambda r: r["target_rate"]):
        b = r["batch_ms"]
        cpu = r["resources"].get("spark-worker", {}).get("cpu_pct_mean")
        lines.append(
            f"| {r['target_rate']:.0f} | {r['producer_rate'] or 0:.0f} | {r['throughput_eps']:.0f} | "
            f"{s(r['processing_latency_ms'])} | {s(r['event_to_detection_ms'])} / {s(r['event_to_detection_p95_ms'])} | "
            f"{s(b['validator'])} / {s(b['aggregator'])} / {s(b['detector'])} | "
            f"{'–' if cpu is None else f'{cpu:.0f}%'} | {'yes' if r['sustainable'] else '**no**'} |")
    return "\n".join(lines)


def main() -> None:
    parts = ["# StreamPulse benchmark report", "",
             "Generated by `python -m benchmarks.generate_reports`. All numbers are measurements on the "
             "machine described below, not guarantees.", ""]
    det_path = RESULTS / "detection_evaluation.json"
    if det_path.exists():
        det = json.loads(det_path.read_text())
        cfg = det["config"]
        imgs = detection_charts(det)
        parts += ["## Detection quality (offline, reproducible)", "",
                  f"{len(cfg['seeds'])} seeds x {cfg['hours']:g} h x {cfg['devices']} devices "
                  f"({sum(r['events'] for r in det['runs']):,} events, {sum(r['episodes'] for r in det['runs'])} "
                  f"injected anomaly episodes), anomaly rate {cfg['anomaly_rate']} per device-hour, "
                  f"grace {cfg['grace']:.0f} s. Engine throughput: "
                  f"**{det['engine_events_per_second']:,.0f} events/s** on one core with all four detectors.", "",
                  to_markdown(det), ""] + [f"![{i}]({i})" for i in imgs] + [""]
    load_path = RESULTS / "load_test.json"
    if load_path.exists():
        load = json.loads(load_path.read_text())
        imgs = load_charts(load)
        host = load["host"]
        parts += ["## Live pipeline load test", "",
                  f"Docker Desktop with {host.get('docker_cpus')} CPUs / "
                  f"{int(host.get('docker_mem') or 0) / 2**30:.1f} GiB shared by every container on the host; "
                  f"Spark: 1 worker, {load['config'].get('devices')} simulated devices, data-quality faults on. "
                  f"Warm-up {load['config']['warmup']} s, measurement window {load['config']['measure']} s per rate.", "",
                  load_table(load), ""] + [f"![{i}]({i})" for i in imgs] + [""]
    rec_path = RESULTS / "recovery_test.json"
    if rec_path.exists():
        rec = json.loads(rec_path.read_text())
        label = {"spark_kill": "Spark driver SIGKILL", "kafka_restart": "Kafka broker stop/start",
                 "postgres_restart": "PostgreSQL stop/start"}
        parts += ["## Failure recovery", "",
                  f"Simulator at {rec['simulation']['rate']:.0f} events/s, {rec['simulation']['devices']} devices. "
                  "Completeness = events counted in every 1-minute window spanning the failure vs. what the "
                  "simulator sent; the ~0.5% gap is the injected duplicates, malformed records and events "
                  "delayed beyond the watermark, i.e. the same as in normal operation.", "",
                  "| Failure | Down for | Recovered after restart | Resumed from checkpoint | Windows | "
                  "Event coverage | Duplicate alert keys |", "|---|---:|---:|:---:|---:|---:|---:|"]
        for r in rec["results"]:
            c, d = r["completeness"], r["duplicates"]
            parts.append(f"| {label.get(r['scenario'], r['scenario'])} | {r['outage_s']} s | "
                         f"{r['recovery_after_restart_s']:.0f} s | {'yes' if r['resumed_from_checkpoint'] else 'no'} | "
                         f"{c['windows_found']}/{c['windows_expected']} | {100 * c['event_coverage']:.1f}% | "
                         f"{d['duplicate_keys']} |")
        parts += ["", "Alerts less than 1 s apart on the same device/metric/algorithm were all a severity "
                  "escalation on a different event (the cooldown lets a more severe detection through), "
                  "not duplicates.", ""]
    (RESULTS / "REPORT.md").write_text("\n".join(parts), encoding="utf-8")
    print("written", RESULTS / "REPORT.md")


if __name__ == "__main__":
    main()
