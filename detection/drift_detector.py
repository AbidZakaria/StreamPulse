"""Detector C: adaptive two-window behavioural drift detector.

Compares a *reference* window (how the device used to behave) with a sliding *recent*
window (how it behaves now) using robust statistics:

* location: ``(median_recent - median_ref) / robust_sigma_ref``
* scale:    ``robust_sigma_recent / robust_sigma_ref``, where
  ``robust_sigma = 1.4826 * MAD``

Medians and MADs ignore isolated spikes, so this detector reacts to changes in the
*distribution* (a machine slowly running hotter, vibration becoming noisier as a bearing
wears) and not to point anomalies. A drift is only raised after ``confirmations``
consecutive positive checks. After a confirmed drift the recent window becomes the new
reference: the detector adapts to the new concept instead of alarming forever.

This module is deliberately independent of Spark and of the other detectors so research
algorithms (e.g. an evolving-stream / AbruptCUSUM-style method) can be dropped in behind
the same :class:`~detection.base_detector.BaseDetector` interface and benchmarked
against the baselines without touching the ingestion pipeline.
"""

from __future__ import annotations

from statistics import median

from detection.base_detector import KIND_DRIFT, METRIC_UNITS, BaseDetector, Finding, fmt, severity_from_ratio

MAD_TO_SIGMA = 1.4826


def robust_location_scale(values: list[float]) -> tuple[float, float]:
    m = median(values)
    mad = median(abs(v - m) for v in values)
    return m, max(MAD_TO_SIGMA * mad, 1e-6)


def slope_per_sample(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den else 0.0


class DriftDetector(BaseDetector):
    name = "drift"
    kind = KIND_DRIFT

    def init_state(self) -> dict:
        return {"ref_buf": [], "ref_loc": None, "ref_scale": None, "recent": [], "since_check": 0,
                "confirm": 0, "drifts": 0}

    def update(self, state: dict, value: float, metric: str) -> Finding | None:
        p = self.params
        if state["ref_loc"] is None:
            state["ref_buf"].append(value)
            if len(state["ref_buf"]) >= p["reference_size"]:
                state["ref_loc"], state["ref_scale"] = robust_location_scale(state["ref_buf"])
                state["ref_buf"] = []
            return None

        recent = state["recent"]
        recent.append(value)
        if len(recent) > p["recent_size"]:
            del recent[0]
        state["since_check"] += 1
        if len(recent) < p["recent_size"] or state["since_check"] < p["check_every"]:
            return None
        state["since_check"] = 0

        loc, scale = robust_location_scale(recent)
        ref_loc, ref_scale = state["ref_loc"], state["ref_scale"]
        shift = (loc - ref_loc) / ref_scale
        ratio = scale / ref_scale
        spread = max(ratio, 1.0 / ratio)
        positive = abs(shift) > p["location_shift"] or spread > p["scale_ratio"]
        state["confirm"] = state["confirm"] + 1 if positive else 0
        if state["confirm"] < p["confirmations"]:
            return None

        strength = max(abs(shift) / p["location_shift"], spread / p["scale_ratio"])
        trend = slope_per_sample(recent) * 100
        parts = []
        if abs(shift) > p["location_shift"]:
            parts.append(f"median moved {fmt(ref_loc, metric)} -> {fmt(loc, metric)} ({shift:+.1f} sigma)")
        if spread > p["scale_ratio"]:
            parts.append(f"variability x{ratio:.2f}")
        finding = Finding(
            observed=loc,
            baseline=ref_loc,
            score=round(max(abs(shift), spread), 3),
            severity=severity_from_ratio(strength),
            explanation=(
                f"Behavioural drift in {metric}: " + "; ".join(parts)
                + f"; trend {trend:+.2f} {METRIC_UNITS.get(metric, '')} per 100 samples. "
                "The reference baseline has been re-learned from recent behaviour."
            ),
        )
        # Adapt: the recent behaviour becomes the new reference concept.
        state["ref_loc"], state["ref_scale"] = loc, scale
        state["recent"] = []
        state["confirm"] = 0
        state["drifts"] += 1
        return finding
