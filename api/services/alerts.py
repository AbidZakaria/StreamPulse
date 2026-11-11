"""Alert queries and lifecycle transitions."""

from __future__ import annotations

from datetime import datetime

from api.db import fetch_all, fetch_one

ALERT_FIELDS = """
    alert_id, device_id, plant_id, metric_name, algorithm, alert_kind, observed_value,
    baseline_value, deviation_score, severity, description, event_id, detected_at,
    persisted_at, status, confirmed_failure, acknowledged_at, acknowledged_by,
    resolved_at, resolution_note
"""


# Stable per-device ordinal: the drawing "item number" shown in the dashboard's balloons.
# It does not change with the time range being viewed (uses alerts_device_idx).
DEVICE_SEQ = """
    (SELECT count(*) FROM alerts b
      WHERE b.device_id = alerts.device_id
        AND (b.detected_at, b.alert_id) <= (alerts.detected_at, alerts.alert_id)) AS device_seq
"""


def _filters(
    device_id: str | None,
    severity: list[str] | None,
    algorithm: list[str] | None,
    status: list[str] | None,
    kind: str | None,
    metric: str | None,
    start: datetime | None,
    end: datetime | None,
) -> tuple[str, dict]:
    clauses, params = [], {}
    if device_id:
        clauses.append("device_id = %(device_id)s")
        params["device_id"] = device_id
    if severity:
        clauses.append("severity = ANY(%(severity)s)")
        params["severity"] = severity
    if algorithm:
        clauses.append("algorithm = ANY(%(algorithm)s)")
        params["algorithm"] = algorithm
    if status:
        clauses.append("status = ANY(%(status)s)")
        params["status"] = status
    if kind:
        clauses.append("alert_kind = %(kind)s")
        params["kind"] = kind
    if metric:
        clauses.append("metric_name = %(metric)s")
        params["metric"] = metric
    if start:
        clauses.append("detected_at >= %(start)s")
        params["start"] = start
    if end:
        clauses.append("detected_at < %(end)s")
        params["end"] = end
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def list_alerts(limit: int, offset: int, **filters) -> dict:
    where, params = _filters(**filters)
    params.update(limit=limit, offset=offset)
    total = fetch_one(f"SELECT count(*) AS n FROM alerts {where}", params)["n"]
    items = fetch_all(
        f"SELECT {ALERT_FIELDS}, {DEVICE_SEQ} FROM alerts {where} "
        "ORDER BY detected_at DESC, alert_id LIMIT %(limit)s OFFSET %(offset)s",
        params,
    )
    return {"total": total, "limit": limit, "offset": offset, "items": items}


def get_alert(alert_id: str) -> dict | None:
    return fetch_one(f"SELECT {ALERT_FIELDS}, {DEVICE_SEQ} FROM alerts WHERE alert_id = %s", (alert_id,))


def acknowledge(alert_id: str, by: str) -> dict | None:
    return fetch_one(
        f"""
        UPDATE alerts
           SET status = CASE WHEN status = 'open' THEN 'acknowledged' ELSE status END,
               acknowledged_at = COALESCE(acknowledged_at, now()),
               acknowledged_by = COALESCE(acknowledged_by, %s)
         WHERE alert_id = %s
        RETURNING {ALERT_FIELDS}
        """,
        (by, alert_id),
    )


def resolve(alert_id: str, confirmed_failure: bool | None, note: str | None) -> dict | None:
    return fetch_one(
        f"""
        UPDATE alerts
           SET status = 'resolved',
               resolved_at = COALESCE(resolved_at, now()),
               acknowledged_at = COALESCE(acknowledged_at, now()),
               confirmed_failure = %s,
               resolution_note = %s
         WHERE alert_id = %s
        RETURNING {ALERT_FIELDS}
        """,
        (confirmed_failure, note, alert_id),
    )


def summary(hours: int) -> dict:
    params = {"hours": hours}
    window = "detected_at >= now() - make_interval(hours => %(hours)s)"

    def grouped(column: str) -> dict:
        rows = fetch_all(f"SELECT {column} AS k, count(*) AS n FROM alerts WHERE {window} GROUP BY 1", params)
        return {r["k"]: r["n"] for r in rows}

    timeline = fetch_all(
        f"""
        SELECT date_bin('5 minutes', detected_at, TIMESTAMPTZ '2000-01-01') AS bucket,
               severity, count(*) AS n
          FROM alerts WHERE {window}
         GROUP BY 1, 2 ORDER BY 1
        """,
        params,
    )
    top_devices = fetch_all(
        f"""
        SELECT device_id, count(*) AS alerts,
               count(*) FILTER (WHERE status <> 'resolved') AS unresolved
          FROM alerts WHERE {window}
         GROUP BY 1 ORDER BY 2 DESC LIMIT 8
        """,
        params,
    )
    return {
        "hours": hours,
        "total": sum(grouped("status").values()),
        "by_severity": grouped("severity"),
        "by_algorithm": grouped("algorithm"),
        "by_kind": grouped("alert_kind"),
        "by_status": grouped("status"),
        "by_metric": grouped("metric_name"),
        "timeline": timeline,
        "top_devices": top_devices,
    }


def live_evaluation(hours: int, grace_seconds: int = 180) -> dict:
    """Score persisted alerts against the simulator's ground truth (injected_anomalies).

    An alert is a true positive when it falls inside an injected episode on the same
    device (episode end extended by ``grace_seconds`` to allow for detection delay).
    """
    params = {"hours": hours, "grace": grace_seconds}
    per_algorithm = fetch_all(
        """
        WITH a AS (
            SELECT a.algorithm, a.alert_id,
                   EXISTS (
                       SELECT 1 FROM injected_anomalies i
                        WHERE i.device_id = a.device_id
                          AND a.detected_at >= i.started_at - interval '1 second'
                          AND a.detected_at <  i.ended_at + make_interval(secs => %(grace)s)
                   ) AS matched
              FROM alerts a
             WHERE a.detected_at >= now() - make_interval(hours => %(hours)s)
        )
        SELECT algorithm, count(*) AS alerts, count(*) FILTER (WHERE matched) AS matched
          FROM a GROUP BY 1 ORDER BY 1
        """,
        params,
    )
    recall = fetch_all(
        """
        WITH eps AS (
            SELECT * FROM injected_anomalies
             WHERE started_at >= now() - make_interval(hours => %(hours)s)
               AND ended_at + make_interval(secs => %(grace)s) < now()
        )
        SELECT alg.algorithm, e.anomaly_type, count(*) AS episodes,
               count(*) FILTER (WHERE d.first_alert IS NOT NULL) AS detected,
               avg(GREATEST(0, EXTRACT(EPOCH FROM d.first_alert - e.started_at)))
                   FILTER (WHERE d.first_alert IS NOT NULL) AS mean_delay_s
          FROM eps e
         CROSS JOIN (VALUES ('threshold'), ('zscore'), ('cusum'), ('drift')) AS alg(algorithm)
          LEFT JOIN LATERAL (
              SELECT min(a.detected_at) AS first_alert FROM alerts a
               WHERE a.device_id = e.device_id AND a.algorithm = alg.algorithm
                 AND a.detected_at >= e.started_at - interval '1 second'
                 AND a.detected_at < e.ended_at + make_interval(secs => %(grace)s)
          ) d ON true
         GROUP BY 1, 2 ORDER BY 1, 2
        """,
        params,
    )
    return {"hours": hours, "grace_seconds": grace_seconds, "precision": per_algorithm, "recall": recall}
