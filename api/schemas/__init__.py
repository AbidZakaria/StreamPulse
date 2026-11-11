"""Request / response models."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

Severity = Literal["low", "medium", "high", "critical"]
Algorithm = Literal["threshold", "zscore", "cusum", "drift"]
AlertStatus = Literal["open", "acknowledged", "resolved"]


class Alert(BaseModel):
    alert_id: str
    device_id: str
    plant_id: str | None = None
    metric_name: str
    algorithm: str
    alert_kind: str
    observed_value: float | None = None
    baseline_value: float | None = None
    deviation_score: float | None = None
    severity: Severity
    description: str
    event_id: str
    detected_at: datetime
    persisted_at: datetime
    status: AlertStatus
    confirmed_failure: bool | None = None
    acknowledged_at: datetime | None = None
    acknowledged_by: str | None = None
    resolved_at: datetime | None = None
    resolution_note: str | None = None
    device_seq: int | None = Field(default=None, description="Stable per-device alert number (balloon number)")


class AlertPage(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[Alert]


class AcknowledgeRequest(BaseModel):
    acknowledged_by: str = Field(default="operator", max_length=100)


class ResolveRequest(BaseModel):
    confirmed_failure: bool | None = Field(
        default=None,
        description="true = the alert matched a real failure, false = false alarm, null = unknown",
    )
    note: str | None = Field(default=None, max_length=2000)


class SimulationStart(BaseModel):
    devices: int | None = Field(default=None, ge=1, le=1000)
    rate: float | None = Field(default=None, gt=0, le=50000, description="total events / second")
    seed: int | None = None
    anomaly_rate: float | None = Field(default=None, ge=0, le=60, description="episodes per device-hour")
    intensity: float | None = Field(default=None, gt=0, le=5)
    data_quality: bool | None = None


class SimulationState(BaseModel):
    running: bool
    devices: int
    rate: float
    seed: int
    anomaly_rate: float
    intensity: float
    data_quality: bool
    config_version: int
    updated_at: datetime
    producer_heartbeat_at: datetime | None
    producer_alive: bool
    events_sent: int
    producer_rate: float | None
