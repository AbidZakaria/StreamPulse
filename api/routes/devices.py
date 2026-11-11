from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from api.services import devices as svc

router = APIRouter(prefix="/devices", tags=["devices"])


@router.get("", summary="Registered devices with latest window metrics and open alerts")
def list_devices(
    plant_id: str | None = None,
    active_within_minutes: int | None = Query(default=None, ge=1, le=10080,
                                              description="only devices with data in the last N minutes"),
):
    return svc.list_devices(plant_id, active_within_minutes)


@router.get("/{device_id}")
def get_device(device_id: str):
    device = svc.get_device(device_id)
    if device is None:
        raise HTTPException(status_code=404, detail="device not found")
    return device


@router.get("/{device_id}/metrics", summary="Sliding-window telemetry history")
def device_metrics(
    device_id: str,
    minutes: int = Query(default=60, ge=1, le=10080),
    limit: int = Query(default=2000, ge=1, le=20000),
):
    if svc.get_device(device_id) is None:
        raise HTTPException(status_code=404, detail="device not found")
    return {"device_id": device_id, "minutes": minutes, "windows": svc.device_metrics(device_id, minutes, limit)}
