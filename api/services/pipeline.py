"""Pipeline health and throughput, derived from what the streaming queries persist."""

from __future__ import annotations

from datetime import datetime, timezone

from api.db import fetch_all, fetch_one

QUERIES = ("validator", "aggregator", "detector")
# A query is "stalled" when it has not committed a batch for this long. Detection and
# aggregation triggers are 5 s / 10 s, so 60 s means several missed triggers.
STALL_AFTER_S = 60
PRODUCER_STALE_S = 20


def _age(ts: datetime | None) -> float | None:
    if ts is None:
        return None
    return (datetime.now(timezone.utc) - ts).total_seconds()


def status() -> dict:
    rows = fetch_all(
        """
        SELECT DISTINCT ON (query_name) query_name, batch_id, recorded_at, batch_duration_ms,
               input_rows_per_sec, processed_rows_per_sec
          FROM pipeline_metrics ORDER BY query_name, recorded_at DESC
        """
    )
    latest = {r["query_name"]: r for r in rows}
    queries = []
    for q in QUERIES:
        r = latest.get(q)
        age = _age(r["recorded_at"]) if r else None
        state = "not_started" if r is None else ("running" if age <= STALL_AFTER_S else "stalled")
        queries.append({"name": q, "state": state, "last_batch_id": r and r["batch_id"],
                        "last_batch_at": r and r["recorded_at"], "seconds_since_last_batch": age,
                        "last_batch_duration_ms": r and r["batch_duration_ms"],
                        "input_rows_per_sec": r and r["input_rows_per_sec"]})

    control = fetch_one("SELECT running, producer_heartbeat_at, producer_rate FROM simulation_control WHERE id = 1")
    beat_age = _age(control["producer_heartbeat_at"]) if control else None
    producer_alive = beat_age is not None and beat_age <= PRODUCER_STALE_S
    producer = {
        "alive": producer_alive,
        "simulation_running": bool(control and control["running"]),
        "rate": control and control["producer_rate"],
        "seconds_since_heartbeat": beat_age,
    }
    states = [q["state"] for q in queries]
    if all(s == "running" for s in states) and producer_alive:
        overall = "healthy"
    elif any(s == "running" for s in states):
        overall = "degraded"
    else:
        overall = "down"
    return {"status": overall, "queries": queries, "producer": producer,
            "checked_at": datetime.now(timezone.utc)}


def metrics(minutes: int, bucket_seconds: int) -> dict:
    params = {"minutes": minutes, "bucket": bucket_seconds}
    series = fetch_all(
        """
        SELECT date_bin(make_interval(secs => %(bucket)s), recorded_at, TIMESTAMPTZ '2000-01-01') AS bucket,
               query_name,
               sum(events_processed)            AS events_processed,
               sum(events_rejected)             AS events_rejected,
               sum(alerts_generated)            AS alerts_generated,
               max(active_devices)              AS active_devices,
               avg(processing_latency_ms)       AS processing_latency_ms,
               avg(end_to_end_latency_ms)       AS end_to_end_latency_ms,
               avg(input_rows_per_sec)          AS input_rows_per_sec,
               avg(processed_rows_per_sec)      AS processed_rows_per_sec,
               avg(batch_duration_ms)           AS batch_duration_ms,
               count(*)                         AS batches
          FROM pipeline_metrics
         WHERE recorded_at >= now() - make_interval(mins => %(minutes)s)
         GROUP BY 1, 2 ORDER BY 1, 2
        """,
        params,
    )
    totals = fetch_one(
        """
        SELECT coalesce(sum(events_processed) FILTER (WHERE query_name = 'validator'), 0) AS events_processed,
               coalesce(sum(events_rejected)  FILTER (WHERE query_name = 'validator'), 0) AS events_rejected,
               coalesce(sum(alerts_generated) FILTER (WHERE query_name = 'detector'), 0)  AS alerts_generated,
               count(*) AS batches
          FROM pipeline_metrics
        """
    )
    recent = fetch_one(
        """
        SELECT sum(events_processed + events_rejected)
                   / greatest(extract(epoch FROM max(recorded_at) - min(recorded_at)), 1) AS ingest_rate,
               avg(processing_latency_ms) AS processing_latency_ms,
               avg(end_to_end_latency_ms) AS end_to_end_latency_ms
          FROM pipeline_metrics
         WHERE query_name = 'validator' AND recorded_at >= now() - interval '60 seconds'
        """
    )
    detector = fetch_one(
        """
        SELECT avg(end_to_end_latency_ms) AS detection_lag_ms, max(active_devices) AS active_devices,
               avg(batch_duration_ms) AS batch_duration_ms
          FROM pipeline_metrics
         WHERE query_name = 'detector' AND recorded_at >= now() - interval '60 seconds'
        """
    )
    alert_latency = fetch_one(
        """
        SELECT avg(extract(epoch FROM persisted_at - detected_at)) * 1000 AS alert_latency_ms
          FROM alerts WHERE persisted_at >= now() - interval '5 minutes'
        """
    )
    rejected = fetch_all(
        """
        SELECT reason, count(*) AS n FROM dead_letter_events
         WHERE received_at >= now() - make_interval(mins => %(minutes)s)
         GROUP BY 1 ORDER BY 2 DESC
        """,
        params,
    )
    return {
        "window_minutes": minutes,
        "bucket_seconds": bucket_seconds,
        "totals": totals,
        "current": {**(recent or {}), **(detector or {}), **(alert_latency or {})},
        "rejections_by_reason": rejected,
        "series": series,
    }


def dead_letters(limit: int) -> list[dict]:
    return fetch_all(
        "SELECT id, received_at, reason, left(raw_payload, 300) AS raw_payload, kafka_partition, kafka_offset "
        "FROM dead_letter_events ORDER BY received_at DESC LIMIT %s",
        (limit,),
    )
