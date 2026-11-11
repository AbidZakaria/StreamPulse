r"""Detector A: adaptive Z-score.

    Z = (x - mu) / sigma

``mu`` and ``sigma`` are exponentially weighted (EWMA) estimates, so the baseline follows
slow, legitimate changes in a device's operating point. Before a sample updates the
baseline it is clipped to ``mu +/- clip * sigma``; otherwise a single spike would inflate
sigma and mask the next anomaly.

Trade-off made visible by the benchmarks: because the baseline adapts, a *slow* drift is
absorbed into it and never flagged. That is exactly the gap CUSUM and the drift detector
cover.
"""

from __future__ import annotations

import math

from detection.base_detector import KIND_POINT, BaseDetector, Finding, Welford, fmt, severity_from_ratio


class ZScoreDetector(BaseDetector):
    name = "zscore"
    kind = KIND_POINT

    def init_state(self) -> dict:
        return {"warm": Welford.init(), "mean": None, "var": None}

    def update(self, state: dict, value: float, metric: str) -> Finding | None:
        p = self.params
        if state["mean"] is None:
            Welford.add(state["warm"], value)
            if state["warm"]["n"] >= p["warmup"]:
                state["mean"] = state["warm"]["mean"]
                state["var"] = max(Welford.std(state["warm"]) ** 2, 1e-12)
                state["warm"] = None
            return None

        mean, var = state["mean"], state["var"]
        sd = max(math.sqrt(var), 1e-6)
        z = (value - mean) / sd

        finding = None
        if abs(z) > p["threshold"]:
            finding = Finding(
                observed=value,
                baseline=mean,
                score=round(abs(z), 3),
                severity=severity_from_ratio(abs(z) / p["threshold"]),
                explanation=(
                    f"{metric} = {fmt(value, metric)} deviates {z:+.1f} sigma from its rolling "
                    f"baseline {fmt(mean, metric)} (sigma = {fmt(sd, metric)})"
                ),
            )

        # Robust EWMA update.
        clip = p["clip"] * sd
        x = min(max(value, mean - clip), mean + clip)
        alpha = p["alpha"]
        diff = x - mean
        state["mean"] = mean + alpha * diff
        state["var"] = max((1 - alpha) * (var + alpha * diff * diff), 1e-12)
        return finding
