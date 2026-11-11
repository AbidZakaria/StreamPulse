"""StreamPulse control and monitoring API.

The API reads what the pipeline has *persisted* in PostgreSQL and never talks to Spark
directly. The streaming job and the API can therefore fail, scale and deploy
independently.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import db
from api.routes import alerts, devices, pipeline, simulation


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.open_pool()
    yield
    db.close_pool()


app = FastAPI(
    title="StreamPulse Engine API",
    version="0.1.0",
    description="Pipeline status, telemetry, anomaly alerts and simulator control for StreamPulse Engine.",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST"], allow_headers=["*"])


@app.get("/health", tags=["health"], summary="API liveness and database connectivity")
def health():
    try:
        db.fetch_one("SELECT 1 AS ok")
        database = "up"
    except Exception:  # noqa: BLE001
        database = "down"
    return {"status": "ok", "database": database}


app.include_router(pipeline.router)
app.include_router(devices.router)
app.include_router(alerts.router)
app.include_router(simulation.router)
