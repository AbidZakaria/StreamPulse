"""Streaming anomaly and drift detection (pure Python, Spark-agnostic)."""

from detection.base_detector import DetectionResult, Finding
from detection.engine import Alert, DetectionEngine, load_detection_config

__all__ = ["Alert", "DetectionEngine", "DetectionResult", "Finding", "load_detection_config"]
