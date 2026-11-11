"""Detector 0: fixed operating limits.

Stateless and trivially explainable. It only fires on extreme values, which makes it the
reference point the statistical detectors are compared against.
"""

from __future__ import annotations

from detection.base_detector import KIND_POINT, BaseDetector, Finding, fmt


class ThresholdDetector(BaseDetector):
    name = "threshold"
    kind = KIND_POINT

    def init_state(self) -> dict:
        return {}

    def update(self, state: dict, value: float, metric: str) -> Finding | None:
        limits = self.params["limits"].get(metric)
        if not limits:
            return None
        low, high = limits["min"], limits["max"]
        if low <= value <= high:
            return None
        band = max(high - low, 1e-9)
        if value > high:
            limit, direction, excess = high, "above", value - high
        else:
            limit, direction, excess = low, "below", low - value
        frac = excess / band
        severity = "medium" if frac < 0.05 else "high" if frac < 0.15 else "critical"
        return Finding(
            observed=value,
            baseline=limit,
            score=round(frac, 4),
            severity=severity,
            explanation=(
                f"{metric} = {fmt(value, metric)} is {direction} the operating limit "
                f"{fmt(limit, metric)} by {fmt(excess, metric)}"
            ),
        )
