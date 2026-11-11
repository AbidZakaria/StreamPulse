"""Detector contract.

Detectors are *pure functions over explicit state*: ``update(state, value)`` mutates a
plain ``dict`` that is JSON-serializable. That single design choice is what lets the
same detector code run

* inside Spark's ``applyInPandasWithState`` (state is checkpointed by Spark and survives
  restarts),
* in the offline benchmark harness, and
* in unit tests,

without any adapter layer.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass

SEVERITIES = ("low", "medium", "high", "critical")
SEVERITY_RANK = {s: i for i, s in enumerate(SEVERITIES)}

KIND_POINT = "point_anomaly"
KIND_SHIFT = "persistent_shift"
KIND_DRIFT = "concept_drift"

METRIC_UNITS = {
    "temperature": "°C",
    "vibration": "mm/s",
    "pressure": "kPa",
    "rpm": "rpm",
    "power_consumption": "kW",
}


@dataclass
class Finding:
    """What a detector reports for a single (metric, sample)."""

    observed: float
    baseline: float | None
    score: float
    severity: str
    explanation: str


@dataclass
class DetectionResult:
    """Standardized output, identical for every algorithm."""

    device_id: str
    plant_id: str | None
    metric: str
    algorithm: str
    kind: str
    observed: float
    baseline: float | None
    score: float
    severity: str
    explanation: str
    event_id: str
    detected_at: float           # event time (epoch seconds) of the triggering sample

    def to_dict(self) -> dict:
        return asdict(self)


def severity_from_ratio(ratio: float, cuts: tuple[float, float, float] = (1.5, 2.5, 4.0)) -> str:
    """Map "how many times over the detection threshold" to a severity level."""
    if ratio < cuts[0]:
        return "low"
    if ratio < cuts[1]:
        return "medium"
    if ratio < cuts[2]:
        return "high"
    return "critical"


def fmt(value: float | None, metric: str) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "n/a"
    unit = METRIC_UNITS.get(metric, "")
    digits = 0 if metric == "rpm" else 2
    return f"{value:.{digits}f} {unit}".strip()


class Welford:
    """Numerically stable running mean / variance on a dict (so it can live in state)."""

    @staticmethod
    def init() -> dict:
        return {"n": 0, "mean": 0.0, "m2": 0.0}

    @staticmethod
    def add(acc: dict, x: float) -> None:
        acc["n"] += 1
        delta = x - acc["mean"]
        acc["mean"] += delta / acc["n"]
        acc["m2"] += delta * (x - acc["mean"])

    @staticmethod
    def std(acc: dict) -> float:
        return math.sqrt(acc["m2"] / (acc["n"] - 1)) if acc["n"] > 1 else 0.0


class BaseDetector(ABC):
    name: str = "base"
    kind: str = KIND_POINT

    def __init__(self, params: dict):
        self.params = params

    @abstractmethod
    def init_state(self) -> dict:
        """Fresh state for one (device, metric) stream."""

    @abstractmethod
    def update(self, state: dict, value: float, metric: str) -> Finding | None:
        """Consume one sample; return a :class:`Finding` when the sample is anomalous."""
