from __future__ import annotations

from fastapi import APIRouter

from api.schemas import SimulationStart, SimulationState
from api.services import simulation as svc

router = APIRouter(prefix="/simulation", tags=["simulation"])


@router.get("/status", response_model=SimulationState)
def simulation_status():
    return svc.get_state()


@router.post("/start", response_model=SimulationState,
             summary="Start telemetry generation (optionally with new parameters)")
def simulation_start(body: SimulationStart | None = None):
    return svc.start((body or SimulationStart()).model_dump())


@router.post("/stop", response_model=SimulationState, summary="Stop telemetry generation")
def simulation_stop():
    return svc.stop()
