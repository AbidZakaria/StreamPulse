"""Telemetry event contract shared by the producer, the Spark validator and the tests.

The Spark job implements the same rules with column expressions
(:mod:`streaming.validation`); :func:`validate_event` is the pure-Python reference used
in unit tests to keep both implementations honest.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = 1

NUMERIC_FIELDS: tuple[str, ...] = ("temperature", "vibration", "pressure", "rpm", "power_consumption")
REQUIRED_FIELDS: tuple[str, ...] = ("event_id", "device_id", "event_time")
STATUS_VALUES: tuple[str, ...] = ("running", "idle", "maintenance")

# Physically plausible ranges. Values outside are sensor/transport errors (rejected to the
# dead-letter topic), not anomalies. Anomaly limits live in config/detection_config.yaml.
PHYSICAL_RANGES: dict[str, tuple[float, float]] = {
    "temperature": (-50.0, 250.0),
    "vibration": (0.0, 100.0),
    "pressure": (0.0, 400.0),
    "rpm": (0.0, 10000.0),
    "power_consumption": (0.0, 1000.0),
}

# Rejection reasons, in the order the validator evaluates them.
REASON_MALFORMED_JSON = "malformed_json"
REASON_MISSING_FIELD = "missing_required_field"
REASON_BAD_TIMESTAMP = "invalid_event_time"
REASON_BAD_TYPE = "non_numeric_measurement"
REASON_OUT_OF_RANGE = "out_of_physical_range"
REASON_NO_MEASUREMENTS = "no_measurements"


def format_event_time(epoch_seconds: float) -> str:
    """ISO-8601 UTC with millisecond precision, e.g. ``2026-11-10T09:30:15.250Z``."""
    dt = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def parse_event_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def as_number(value: Any) -> float | None:
    """Coerce a measurement to float, or return None if it is not numeric.

    Numeric strings ("78.5") are accepted, matching Spark's ``try_cast`` semantics:
    Spark's JSON reader cannot tell ``78.5`` from ``"78.5"`` when the field is read as
    STRING, so both implementations deliberately treat them the same way.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def validate_event(payload: str | dict) -> tuple[bool, str | None]:
    """Return ``(True, None)`` for a valid event, otherwise ``(False, reason)``.

    Missing *measurements* (``null`` or absent) are allowed: they are counted in the
    missing-measurement rate rather than rejected. An event with no measurements at all
    carries no information and is rejected.
    """
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return False, REASON_MALFORMED_JSON
    if not isinstance(payload, dict):
        return False, REASON_MALFORMED_JSON

    for name in REQUIRED_FIELDS:
        value = payload.get(name)
        if value is None or (isinstance(value, str) and not value.strip()):
            return False, REASON_MISSING_FIELD

    if parse_event_time(payload.get("event_time")) is None:
        return False, REASON_BAD_TIMESTAMP

    present = 0
    for name in NUMERIC_FIELDS:
        value = payload.get(name)
        if value is None:
            continue
        number = as_number(value)
        if number is None:
            return False, REASON_BAD_TYPE
        low, high = PHYSICAL_RANGES[name]
        if not (low <= number <= high):
            return False, REASON_OUT_OF_RANGE
        present += 1

    if present == 0:
        return False, REASON_NO_MEASUREMENTS
    return True, None
