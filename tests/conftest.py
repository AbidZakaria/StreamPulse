from __future__ import annotations

import pytest

from detection.engine import load_detection_config
from producer.anomaly_injector import DataQualityConfig
from producer.simulator import SimulationConfig, TelemetrySimulator

T0 = 1_700_000_000.0


@pytest.fixture
def detection_config() -> dict:
    return load_detection_config()


@pytest.fixture
def make_simulator():
    def _make(**overrides) -> TelemetrySimulator:
        dq = overrides.pop("data_quality", DataQualityConfig(enabled=False))
        cfg = SimulationConfig(data_quality=dq, **{"devices": 5, "rate": 5, "seed": 1, **overrides})
        return TelemetrySimulator(cfg, start_time=T0)

    return _make
