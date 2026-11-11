from __future__ import annotations

import json

from detection.alert_manager import AlertManager, make_alert_id
from detection.base_detector import DetectionResult
from detection.engine import DetectionEngine
from producer.event_schema import parse_event_time
from tests.conftest import T0


def result(ts: float, severity: str = "medium", algorithm: str = "threshold", event_id: str = "e") -> DetectionResult:
    return DetectionResult("dev", "p", "temperature", algorithm, "point_anomaly", 99.0, 92.0, 1.0, severity,
                           "x", event_id, ts)


def test_alert_id_is_deterministic():
    a = make_alert_id("d", "temperature", "cusum", "evt-1")
    assert a == make_alert_id("d", "temperature", "cusum", "evt-1")
    assert a != make_alert_id("d", "temperature", "cusum", "evt-2")
    assert a.startswith("al_")


def test_cooldown_suppresses_repeats():
    mgr = AlertManager({"threshold": 60})
    state = mgr.init_state()
    decisions = [mgr.admit(state, result(T0 + s))[0] for s in range(0, 200, 5)]
    # emitted at t=0, 60, 120, 180
    assert sum(decisions) == 4


def test_escalation_bypasses_cooldown():
    mgr = AlertManager({"threshold": 60})
    state = mgr.init_state()
    assert mgr.admit(state, result(T0, "medium"))[0]
    assert not mgr.admit(state, result(T0 + 5, "medium"))[0]
    assert mgr.admit(state, result(T0 + 10, "critical"))[0]
    assert not mgr.admit(state, result(T0 + 15, "high"))[0]


def test_suppressed_count_reported_on_next_alert():
    mgr = AlertManager({"threshold": 60})
    state = mgr.init_state()
    mgr.admit(state, result(T0))
    for s in (1, 2, 3):
        mgr.admit(state, result(T0 + s))
    emit, suppressed = mgr.admit(state, result(T0 + 61))
    assert emit and suppressed == 3


def test_cooldown_is_per_algorithm():
    mgr = AlertManager({"threshold": 60, "zscore": 60})
    state = mgr.init_state()
    assert mgr.admit(state, result(T0, algorithm="threshold"))[0]
    assert mgr.admit(state, result(T0 + 1, algorithm="zscore"))[0]


def _events(sim, duration):
    for m in sim.generate(T0, duration):
        e = dict(m.event)
        e["event_time"] = parse_event_time(e["event_time"]).timestamp()
        yield e


def test_engine_replay_is_deterministic(make_simulator):
    """Replaying the same events from the same state yields identical alert ids, which
    is what makes the ON CONFLICT insert idempotent after a crash."""
    engine = DetectionEngine.from_yaml()
    events = list(_events(make_simulator(devices=3, rate=3, anomaly_rate=30, seed=9), 3600))

    def run(evts, states):
        ids = []
        for e in evts:
            _, alerts = engine.process_event(states.setdefault(e["device_id"], engine.new_device_state()), e)
            ids += [a.alert_id for a in alerts]
        return ids

    first = run(events, {})
    assert first, "expected alerts with a high anomaly rate"

    # checkpoint halfway (serialize state as Spark would), then replay the second half twice
    half = len(events) // 2
    states = {}
    run(events[:half], states)
    snapshot = json.dumps(states)
    replay_1 = run(events[half:], json.loads(snapshot))
    replay_2 = run(events[half:], json.loads(snapshot))
    assert replay_1 == replay_2
    assert first == run(events[:half], {}) + replay_1


def test_engine_skips_missing_measurements():
    engine = DetectionEngine.from_yaml()
    state = engine.new_device_state()
    event = {"device_id": "d", "event_id": "e", "event_time": T0, "temperature": None,
             "vibration": float("nan"), "pressure": 101.0, "rpm": 1500.0, "power_consumption": 4.0}
    engine.process_event(state, event)
    assert state["detectors"]["temperature"]["zscore"]["warm"]["n"] == 0
    assert state["detectors"]["pressure"]["zscore"]["warm"]["n"] == 1


def test_alert_record_has_standard_fields():
    engine = DetectionEngine.from_yaml(algorithms=["threshold"])
    state = engine.new_device_state()
    _, alerts = engine.process_event(state, {"device_id": "d", "plant_id": "p", "event_id": "e1",
                                             "event_time": T0, "temperature": 130.0})
    rec = alerts[0].to_record()
    for key in ("alert_id", "device_id", "metric_name", "algorithm", "alert_kind", "observed_value",
                "baseline_value", "deviation_score", "severity", "description", "detected_at"):
        assert rec[key] is not None
