"""Simulator control plane (the producer polls the simulation_control row)."""

from __future__ import annotations

from datetime import datetime, timezone

from api.db import fetch_one

PRODUCER_STALE_S = 20

SELECT = """
SELECT running, devices, rate, seed, anomaly_rate, intensity, data_quality, config_version,
       updated_at, producer_heartbeat_at, events_sent, producer_rate
  FROM simulation_control WHERE id = 1
"""


def _decorate(row: dict) -> dict:
    beat = row.get("producer_heartbeat_at")
    row["producer_alive"] = beat is not None and (datetime.now(timezone.utc) - beat).total_seconds() <= PRODUCER_STALE_S
    return row


def get_state() -> dict:
    return _decorate(fetch_one(SELECT))


def start(params: dict) -> dict:
    """Start (or reconfigure) the simulation. Any parameter change bumps config_version,
    which makes the producer rebuild its simulator with the new settings."""
    changes = {k: v for k, v in params.items() if v is not None}
    sets = ["running = true", "updated_at = now()"]
    sets += [f"{k} = %({k})s" for k in changes]
    if changes:
        sets.append("config_version = config_version + 1")
    fetch_one(f"UPDATE simulation_control SET {', '.join(sets)} WHERE id = 1 RETURNING id", changes)
    return get_state()


def stop() -> dict:
    fetch_one("UPDATE simulation_control SET running = false, updated_at = now() WHERE id = 1 RETURNING id")
    return get_state()
