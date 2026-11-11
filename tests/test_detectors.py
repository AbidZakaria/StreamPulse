from __future__ import annotations

import json
import random

import pytest

from detection.cusum_detector import CusumDetector
from detection.drift_detector import DriftDetector
from detection.threshold_detector import ThresholdDetector
from detection.zscore_detector import ZScoreDetector


def gaussian(n: int, mean: float = 50.0, sd: float = 1.0, seed: int = 0) -> list[float]:
    rng = random.Random(seed)
    return [rng.gauss(mean, sd) for _ in range(n)]


def run(detector, values, metric="temperature"):
    state = detector.init_state()
    findings = []
    for i, v in enumerate(values):
        f = detector.update(state, v, metric)
        if f is not None:
            findings.append((i, f))
        json.dumps(state)  # state must stay JSON-serializable at every step
    return findings, state


# ------------------------------------------------------------------------- threshold
def test_threshold_flags_only_limit_breaches(detection_config):
    det = ThresholdDetector(detection_config["threshold"])
    findings, _ = run(det, [60, 70, 91.9, 92.5, 120, 4.0])
    assert [i for i, _ in findings] == [3, 4, 5]
    assert findings[0][1].severity == "medium"
    assert findings[1][1].severity == "critical"
    assert "below" in findings[2][1].explanation


# --------------------------------------------------------------------------- z-score
def test_zscore_warmup_then_detects_spike(detection_config):
    det = ZScoreDetector(detection_config["zscore"])
    values = gaussian(500)
    values[300] += 12
    findings, _ = run(det, values)
    assert [i for i, _ in findings] == [300]
    assert findings[0][1].score > 10


def test_zscore_no_alarm_during_warmup(detection_config):
    det = ZScoreDetector(detection_config["zscore"])
    values = gaussian(50)
    values[10] += 50
    findings, _ = run(det, values)
    assert findings == []


def test_zscore_adapts_to_slow_drift(detection_config):
    """Documented limitation: EWMA absorbs a slow ramp, so z-score stays quiet."""
    det = ZScoreDetector(detection_config["zscore"])
    values = [v + max(0, i - 200) * 0.005 for i, v in enumerate(gaussian(2000))]
    findings, _ = run(det, values)
    assert findings == []


# ----------------------------------------------------------------------------- cusum
def test_cusum_textbook_recursion(detection_config):
    params = {**detection_config["cusum"], "warmup": 2, "clip": 100.0, "h": 4.0, "k": 0.5,
              "rebaseline_after_alarm": False}
    det = CusumDetector(params)
    state = det.init_state()
    det.update(state, 9.0, "x")
    det.update(state, 11.0, "x")          # mu0 = 10, sigma = sqrt(2)
    sigma = 2 ** 0.5
    s = 0.0
    for x in (11.0, 12.0, 10.5):
        det.update(state, x, "x")
        s = max(0.0, s + (x - 10.0) / sigma - 0.5)
        assert state["s_pos"] == pytest.approx(s)


def test_cusum_detects_small_persistent_shift(detection_config):
    det = CusumDetector(detection_config["cusum"])
    values = gaussian(600, seed=1)
    shift_at = 300
    values = [v + (1.0 if i >= shift_at else 0.0) for i, v in enumerate(values)]  # +1 sigma
    findings, _ = run(det, values)
    assert findings, "CUSUM should detect a 1-sigma persistent shift"
    first = findings[0][0]
    assert shift_at <= first < shift_at + 60
    f = findings[0][1]
    assert "upward" in f.explanation and f.score > detection_config["cusum"]["h"]


def test_cusum_ignores_isolated_spikes(detection_config):
    det = CusumDetector(detection_config["cusum"])
    values = gaussian(1000, seed=2)
    for i in (300, 500, 700):
        values[i] += 25
    findings, _ = run(det, values)
    assert findings == []


def test_cusum_detects_downward_shift_and_rebaselines(detection_config):
    det = CusumDetector(detection_config["cusum"])
    values = [v - (3.0 if i >= 300 else 0.0) for i, v in enumerate(gaussian(1200, seed=3))]
    findings, state = run(det, values)
    assert len(findings) == 1, "re-baselining should report the shift once, not on every sample"
    assert "downward" in findings[0][1].explanation
    assert state["mu0"] == pytest.approx(47.0, abs=0.5)


def test_cusum_false_alarm_rate_on_noise(detection_config):
    det = CusumDetector(detection_config["cusum"])
    findings, _ = run(det, gaussian(20000, seed=4))
    assert len(findings) <= 1


# ----------------------------------------------------------------------------- drift
def test_drift_detects_gradual_ramp(detection_config):
    det = DriftDetector(detection_config["drift"])
    values = [v + max(0, i - 600) * 0.01 for i, v in enumerate(gaussian(1500, seed=5))]
    findings, _ = run(det, values)
    assert findings and findings[0][0] > 600
    assert findings[0][1].observed > findings[0][1].baseline


def test_drift_detects_variance_increase(detection_config):
    det = DriftDetector(detection_config["drift"])
    rng = random.Random(6)
    values = [rng.gauss(50, 1.0 if i < 600 else 3.0) for i in range(1200)]
    findings, _ = run(det, values)
    assert findings
    assert "variability" in findings[0][1].explanation


def test_drift_ignores_spikes(detection_config):
    det = DriftDetector(detection_config["drift"])
    values = gaussian(2000, seed=7)
    for i in range(400, 2000, 37):
        values[i] += 30
    findings, _ = run(det, values)
    assert findings == []
