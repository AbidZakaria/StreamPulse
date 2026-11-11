from __future__ import annotations

from fastapi import APIRouter, Query

from api.services import pipeline as svc

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.get("/status", summary="Health of each streaming query and of the producer")
def pipeline_status():
    return svc.status()


@router.get("/metrics", summary="Throughput, latency and rejection statistics")
def pipeline_metrics(
    minutes: int = Query(default=15, ge=1, le=1440),
    bucket_seconds: int = Query(default=15, ge=5, le=3600),
):
    return svc.metrics(minutes, bucket_seconds)


@router.get("/dead-letters", summary="Most recent rejected records")
def dead_letters(limit: int = Query(default=50, ge=1, le=500)):
    return svc.dead_letters(limit)
