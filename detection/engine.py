"""Detection engine: runs every enabled detector on every metric of a device's stream.

The engine owns no state itself. The caller holds one JSON-serializable dict per device
(:meth:`DetectionEngine.new_device_state`). Spark keeps it in ``GroupState`` and the
benchmarks keep it in a plain dict.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from detection.alert_manager import AlertManager, make_alert_id
from detection.base_detector import BaseDetector, DetectionResult
from detection.cusum_detector import CusumDetector
from detection.drift_detector import DriftDetector
from detection.threshold_detector import ThresholdDetector
from detection.zscore_detector import ZScoreDetector

DETECTOR_CLASSES: dict[str, type[BaseDetector]] = {
    "threshold": ThresholdDetector,
    "zscore": ZScoreDetector,
    "cusum": CusumDetector,
    "drift": DriftDetector,
}

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "detection_config.yaml"


def load_detection_config(path: str | os.PathLike | None = None) -> dict:
    path = path or os.environ.get("STREAMPULSE_DETECTION_CONFIG") or DEFAULT_CONFIG_PATH
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@dataclass
class Alert:
    alert_id: str
    result: DetectionResult
    suppressed_since_last: int

    def to_record(self) -> dict:
        r = self.result
        return {
            "alert_id": self.alert_id,
            "device_id": r.device_id,
            "plant_id": r.plant_id,
            "metric_name": r.metric,
            "algorithm": r.algorithm,
            "alert_kind": r.kind,
            "observed_value": r.observed,
            "baseline_value": r.baseline,
            "deviation_score": r.score,
            "severity": r.severity,
            "description": r.explanation
            + (f" ({self.suppressed_since_last} similar detections suppressed by cooldown)"
               if self.suppressed_since_last else ""),
            "event_id": r.event_id,
            "detected_at": r.detected_at,
        }


class DetectionEngine:
    def __init__(self, config: dict, algorithms: list[str] | None = None):
        self.config = config
        self.metrics: list[str] = list(config["metrics"])
        names = algorithms or [n for n in DETECTOR_CLASSES if config.get(n, {}).get("enabled", False)]
        self.detectors: dict[str, BaseDetector] = {n: DETECTOR_CLASSES[n](config[n]) for n in names}
        self.alerts = AlertManager(config.get("alerts", {}).get("cooldown_seconds", {}))

    @classmethod
    def from_yaml(cls, path: str | os.PathLike | None = None, algorithms: list[str] | None = None):
        return cls(load_detection_config(path), algorithms)

    def new_device_state(self) -> dict:
        return {
            "detectors": {m: {n: d.init_state() for n, d in self.detectors.items()} for m in self.metrics},
            "alerts": AlertManager.init_state(),
            "events": 0,
            "last_event_time": None,
        }

    def process_event(self, state: dict, event: dict) -> tuple[list[DetectionResult], list[Alert]]:
        """Feed one validated event through every detector.

        ``event`` needs ``device_id``, ``event_id``, ``event_time`` (epoch seconds) and the
        metric values (``None``/NaN = missing measurement, skipped).
        Returns ``(raw_detections, emitted_alerts)``.
        """
        detections: list[DetectionResult] = []
        alerts: list[Alert] = []
        ts = float(event["event_time"])
        state["events"] += 1
        last = state["last_event_time"]
        state["last_event_time"] = ts if last is None else max(last, ts)

        for metric in self.metrics:
            value = event.get(metric)
            if value is None or (isinstance(value, float) and math.isnan(value)):
                continue
            value = float(value)
            metric_state = state["detectors"].setdefault(metric, {})
            for name, detector in self.detectors.items():
                det_state = metric_state.get(name)
                if det_state is None:
                    det_state = metric_state[name] = detector.init_state()
                finding = detector.update(det_state, value, metric)
                if finding is None:
                    continue
                result = DetectionResult(
                    device_id=event["device_id"],
                    plant_id=event.get("plant_id"),
                    metric=metric,
                    algorithm=name,
                    kind=detector.kind,
                    observed=round(finding.observed, 4),
                    baseline=None if finding.baseline is None else round(finding.baseline, 4),
                    score=finding.score,
                    severity=finding.severity,
                    explanation=finding.explanation,
                    event_id=event["event_id"],
                    detected_at=ts,
                )
                detections.append(result)
                emit, suppressed = self.alerts.admit(state["alerts"], result)
                if emit:
                    alert_id = make_alert_id(result.device_id, metric, name, result.event_id)
                    alerts.append(Alert(alert_id, result, suppressed))
        return detections, alerts
