"""Device registry and telemetry history."""

from __future__ import annotations

from api.db import fetch_all, fetch_one

DEVICE_LIST_SQL = """
SELECT d.device_id, d.plant_id, d.device_type, d.operational_status, d.registration_date,
       d.last_seen_at,
       coalesce(a.open_alerts, 0) AS open_alerts,
       a.worst_open_severity,
       m.avg_temperature, m.avg_vibration, m.avg_pressure, m.avg_rpm, m.avg_power_consumption,
       m.event_count AS events_last_window, m.window_end AS last_window_end
  FROM devices d
  LEFT JOIN LATERAL (
      SELECT count(*) AS open_alerts,
             (array_agg(severity ORDER BY CASE severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3
                                                        WHEN 'medium' THEN 2 ELSE 1 END DESC))[1]
                 AS worst_open_severity
        FROM alerts WHERE device_id = d.device_id AND status <> 'resolved'
  ) a ON true
  LEFT JOIN LATERAL (
      SELECT * FROM telemetry_metrics
       WHERE device_id = d.device_id ORDER BY window_end DESC LIMIT 1
  ) m ON true
 {where}
 ORDER BY d.device_id
"""


def list_devices(plant_id: str | None = None, active_within_minutes: int | None = None) -> list[dict]:
    """All registered devices, optionally only those that sent data recently (earlier
    simulator configurations and integration tests leave devices that no longer report)."""
    clauses, params = [], []
    if plant_id:
        clauses.append("d.plant_id = %s")
        params.append(plant_id)
    if active_within_minutes:
        clauses.append("d.last_seen_at >= now() - make_interval(mins => %s)")
        params.append(active_within_minutes)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return fetch_all(DEVICE_LIST_SQL.format(where=where), tuple(params))


def get_device(device_id: str) -> dict | None:
    return fetch_one(DEVICE_LIST_SQL.format(where="WHERE d.device_id = %s"), (device_id,))


def device_metrics(device_id: str, minutes: int, limit: int) -> list[dict]:
    return fetch_all(
        """
        SELECT window_start, window_end, avg_temperature, min_temperature, max_temperature,
               std_temperature, avg_vibration, max_vibration, std_vibration, avg_pressure,
               min_pressure, avg_rpm, avg_power_consumption, event_count, missing_measurement_rate
          FROM telemetry_metrics
         WHERE device_id = %s AND window_end >= now() - make_interval(mins => %s)
         ORDER BY window_end DESC
         LIMIT %s
        """,
        (device_id, minutes, limit),
    )[::-1]
