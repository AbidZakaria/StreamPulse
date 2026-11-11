"""End-to-end tests against the running Docker Compose stack.

    docker compose up -d --build
    STREAMPULSE_INTEGRATION=1 pytest -m integration

They exercise the real path: producer -> Kafka -> Spark -> PostgreSQL -> FastAPI.
"""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest

pytestmark = pytest.mark.integration
if not os.environ.get("STREAMPULSE_INTEGRATION"):
    pytest.skip("set STREAMPULSE_INTEGRATION=1 with the stack running", allow_module_level=True)

httpx = pytest.importorskip("httpx")
API = os.environ.get("STREAMPULSE_API", "http://localhost:8000")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


def get(path: str, **params):
    r = httpx.get(f"{API}{path}", params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def wait_for(predicate, timeout: float, interval: float = 3.0, what: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {what} (last={last!r})")


def produce(messages: list[tuple[str, str]]) -> None:
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
    for key, value in messages:
        p.produce("raw-telemetry", key=key.encode(), value=value.encode())
    assert p.flush(15) == 0


def test_api_health():
    assert get("/health") == {"status": "ok", "database": "up"}


def test_all_streaming_queries_running():
    status = wait_for(lambda: (s := get("/pipeline/status"))["status"] == "healthy" and s, 180,
                      what="healthy pipeline")
    assert {q["name"] for q in status["queries"] if q["state"] == "running"} == {"validator", "aggregator", "detector"}


def test_telemetry_reaches_postgres():
    devices = wait_for(lambda: [d for d in get("/devices") if d["last_window_end"]], 180, what="device windows")
    history = get(f"/devices/{devices[0]['device_id']}/metrics", minutes=30)
    assert history["windows"] and history["windows"][-1]["event_count"] > 0


def test_malformed_record_goes_to_dead_letter():
    marker = f"it-{uuid.uuid4().hex[:10]}"
    produce([("it", "{" + marker)])  # truncated JSON

    def found():
        return [d for d in get("/pipeline/dead-letters", limit=200) if marker in (d["raw_payload"] or "")]

    rows = wait_for(found, 120, what="dead letter")
    assert rows[0]["reason"] == "malformed_json"


def test_injected_spike_produces_alert_end_to_end():
    """A hand-made extreme reading must come out as a threshold alert in the API."""
    device = f"it_{uuid.uuid4().hex[:6]}"
    now = time.time()
    events = []
    for i in range(5):
        t = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now + i)) + ".000Z"
        events.append({"event_id": f"{device}-{i}", "device_id": device, "plant_id": "plant_it", "event_time": t,
                       "temperature": 150.0 if i == 4 else 60.0, "vibration": 2.0, "pressure": 100.0,
                       "rpm": 1500.0, "power_consumption": 4.0})
    produce([(device, json.dumps(e)) for e in events])
    alerts = wait_for(lambda: get("/alerts", device_id=device, algorithm="threshold")["items"], 180,
                      what="threshold alert")
    assert alerts[0]["metric_name"] == "temperature"
    assert alerts[0]["severity"] == "critical"
    # acknowledge + resolve lifecycle
    aid = alerts[0]["alert_id"]
    r = httpx.post(f"{API}/alerts/{aid}/acknowledge", json={"acknowledged_by": "it"}, timeout=10).json()
    assert r["status"] == "acknowledged"
    r = httpx.post(f"{API}/alerts/{aid}/resolve", json={"confirmed_failure": True, "note": "it"}, timeout=10).json()
    assert r["status"] == "resolved" and r["confirmed_failure"] is True


def test_duplicate_events_are_persisted_once():
    """The same event published three times yields one alert row (dedup + idempotent insert)."""
    device = f"it_{uuid.uuid4().hex[:6]}"
    t = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + ".000Z"
    event = {"event_id": f"{device}-dup", "device_id": device, "plant_id": "plant_it", "event_time": t,
             "temperature": 150.0, "vibration": 2.0, "pressure": 100.0, "rpm": 1500.0, "power_consumption": 4.0}
    produce([(device, json.dumps(event))] * 3)
    wait_for(lambda: get("/alerts", device_id=device)["items"], 180, what="alert for duplicated event")
    time.sleep(15)
    assert get("/alerts", device_id=device, algorithm="threshold")["total"] == 1
