from __future__ import annotations

import json
from collections import Counter

import pytest

from producer.anomaly_injector import MALFORMED_KINDS, DataQualityConfig, corrupt
from producer.event_schema import (
    REASON_BAD_TIMESTAMP,
    REASON_BAD_TYPE,
    REASON_MALFORMED_JSON,
    REASON_MISSING_FIELD,
    REASON_NO_MEASUREMENTS,
    REASON_OUT_OF_RANGE,
    format_event_time,
    parse_event_time,
    validate_event,
)
from tests.conftest import T0


def test_events_follow_schema(make_simulator):
    sim = make_simulator()
    messages = list(sim.generate(T0, 60))
    assert len(messages) == 5 * 60
    for m in messages:
        ok, reason = validate_event(m.value)
        assert ok, reason
        event = json.loads(m.value)
        assert m.key == event["device_id"]
        assert set(event) >= {"event_id", "device_id", "plant_id", "event_time", "temperature", "vibration",
                              "pressure", "rpm", "power_consumption"}


def test_same_seed_is_reproducible(make_simulator):
    a = [m.value for m in make_simulator(seed=7, anomaly_rate=30).generate(T0, 600)]
    b = [m.value for m in make_simulator(seed=7, anomaly_rate=30).generate(T0, 600)]
    c = [m.value for m in make_simulator(seed=8, anomaly_rate=30).generate(T0, 600)]
    assert a == b
    assert a != c


def test_rate_controls_volume(make_simulator):
    sim = make_simulator(devices=10, rate=50)
    assert sim.config.interval == pytest.approx(0.2)
    assert len(list(sim.generate(T0, 10))) == 500


def test_event_ids_are_unique_and_namespaced(make_simulator):
    sim = make_simulator(run_id="abc")
    ids = [json.loads(m.value)["event_id"] for m in sim.generate(T0, 120)]
    assert len(ids) == len(set(ids))
    assert all(i.startswith("evt-abc-") for i in ids)


def test_anomalies_are_injected_with_ground_truth(make_simulator):
    sim = make_simulator(devices=10, rate=10, anomaly_rate=20, seed=3)
    list(sim.generate(T0, 3 * 3600))
    types = Counter(e.anomaly_type for e in sim.episodes)
    assert len(types) >= 5, types
    for ep in sim.episodes:
        assert ep.end > ep.start
        probe = ep.start if ep.shape in ("point", "step") else (ep.start + ep.end) / 2
        assert ep.offset(probe) != 0
        assert ep.offset(ep.end + 1) == 0


def test_spike_moves_the_measurement(make_simulator):
    sim = make_simulator(devices=20, rate=20, anomaly_rate=10, seed=11)
    by_time = {(m.event["device_id"], round(m.event_time, 3)): m.event for m in sim.generate(T0, 3600)}
    spikes = [e for e in sim.episodes if e.anomaly_type == "temperature_spike"]
    assert spikes, "expected at least one spike"
    baselines = {p.device_id: p.baselines["temperature"] for p in sim.fleet}
    for ep in spikes:
        event = by_time[(ep.device_id, round(ep.start, 3))]
        assert event["temperature"] - baselines[ep.device_id] > 10


def test_data_quality_faults_are_produced(make_simulator):
    dq = DataQualityConfig(enabled=True, missing_prob=0.05, duplicate_prob=0.05, delay_prob=0.05,
                           malformed_prob=0.05)
    sim = make_simulator(devices=10, rate=20, data_quality=dq, seed=5)
    messages = list(sim.generate(T0, 600))
    kinds = Counter(m.kind for m in messages)
    assert kinds["duplicate"] > 0 and kinds["malformed"] > 0 and kinds["delayed"] > 0
    # delayed events arrive out of order
    times = [m.event_time for m in messages if m.event]
    assert any(b < a for a, b in zip(times, times[1:], strict=False))
    # malformed records are rejected, everything else is valid
    for m in messages:
        ok, _ = validate_event(m.value)
        assert ok == (m.kind != "malformed") or m.kind == "malformed"
    assert all(not validate_event(m.value)[0] for m in messages if m.kind == "malformed")
    # some events carry a missing measurement but are still valid
    missing = [m for m in messages if m.event and any(m.event[k] is None for k in ("temperature", "rpm",
                                                                                  "vibration", "pressure",
                                                                                  "power_consumption"))]
    assert missing and all(validate_event(m.value)[0] for m in missing)


GOOD = {"event_id": "e1", "device_id": "sensor_001", "plant_id": "plant_01",
        "event_time": "2026-11-10T09:30:15.250Z", "temperature": 78.5, "vibration": 2.4,
        "pressure": 101.2, "rpm": 1450, "power_consumption": 4.8}


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (GOOD, None),
        ({**GOOD, "temperature": None}, None),
        ({**GOOD, "temperature": "78.5"}, None),                      # numeric strings coerced
        ("{not json", REASON_MALFORMED_JSON),
        ("[1, 2, 3]", REASON_MALFORMED_JSON),
        ({k: v for k, v in GOOD.items() if k != "device_id"}, REASON_MISSING_FIELD),
        ({**GOOD, "event_id": "  "}, REASON_MISSING_FIELD),
        ({**GOOD, "event_time": "31/02/2026 25:61"}, REASON_BAD_TIMESTAMP),
        ({**GOOD, "temperature": "ERR_SENSOR"}, REASON_BAD_TYPE),
        ({**GOOD, "rpm": True}, REASON_BAD_TYPE),
        ({**GOOD, "rpm": -250}, REASON_OUT_OF_RANGE),
        ({**GOOD, "pressure": 9999}, REASON_OUT_OF_RANGE),
        ({**GOOD, **{k: None for k in ("temperature", "vibration", "pressure", "rpm", "power_consumption")}},
         REASON_NO_MEASUREMENTS),
    ],
)
def test_validate_event(payload, reason):
    ok, got = validate_event(payload if isinstance(payload, str) else json.dumps(payload))
    assert ok == (reason is None)
    assert got == reason


@pytest.mark.parametrize("kind", MALFORMED_KINDS)
def test_every_corruption_is_rejected(kind):
    assert validate_event(corrupt(GOOD, kind))[0] is False


def test_event_time_roundtrip():
    s = format_event_time(T0 + 0.25)
    assert s.endswith("Z") and ".250" in s
    assert parse_event_time(s).timestamp() == pytest.approx(T0 + 0.25)
