from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from api.schemas import AcknowledgeRequest, Alert, AlertPage, AlertStatus, Algorithm, ResolveRequest, Severity
from api.services import alerts as svc

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=AlertPage, summary="List alerts with filters")
def list_alerts(
    device_id: str | None = None,
    severity: list[Severity] | None = Query(default=None),
    algorithm: list[Algorithm] | None = Query(default=None),
    status: list[AlertStatus] | None = Query(default=None),
    kind: str | None = Query(default=None, description="point_anomaly | persistent_shift | concept_drift"),
    metric: str | None = None,
    start: datetime | None = Query(default=None, description="detected_at >= start (ISO-8601)"),
    end: datetime | None = Query(default=None, description="detected_at < end (ISO-8601)"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    return svc.list_alerts(limit, offset, device_id=device_id, severity=severity, algorithm=algorithm,
                           status=status, kind=kind, metric=metric, start=start, end=end)


@router.get("/summary", summary="Alert distribution by severity, algorithm, kind and time")
def alert_summary(hours: int = Query(default=24, ge=1, le=720)):
    return svc.summary(hours)


@router.get("/evaluation", summary="Live precision / recall against injected ground truth")
def alert_evaluation(hours: int = Query(default=24, ge=1, le=720), grace_seconds: int = Query(default=180, ge=0)):
    return svc.live_evaluation(hours, grace_seconds)


@router.get("/{alert_id}", response_model=Alert)
def get_alert(alert_id: str):
    alert = svc.get_alert(alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="alert not found")
    return alert


@router.post("/{alert_id}/acknowledge", response_model=Alert)
def acknowledge(alert_id: str, body: AcknowledgeRequest | None = None):
    alert = svc.acknowledge(alert_id, (body or AcknowledgeRequest()).acknowledged_by)
    if alert is None:
        raise HTTPException(status_code=404, detail="alert not found")
    return alert


@router.post("/{alert_id}/resolve", response_model=Alert,
             summary="Resolve an alert, optionally confirming or rejecting an operational failure")
def resolve(alert_id: str, body: ResolveRequest | None = None):
    body = body or ResolveRequest()
    alert = svc.resolve(alert_id, body.confirmed_failure, body.note)
    if alert is None:
        raise HTTPException(status_code=404, detail="alert not found")
    return alert
