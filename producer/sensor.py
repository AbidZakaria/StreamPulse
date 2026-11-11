"""Simulated industrial devices.

Each device has its own baseline (a pump in one plant runs hotter than another), and
measurements are coupled the way they are on real rotating equipment: higher RPM draws
more power and produces more heat.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

# (low, high) range a device's *baseline mean* is drawn from, and the per-sample noise sd.
BASELINE_RANGES: dict[str, tuple[float, float]] = {
    "temperature": (55.0, 72.0),
    "vibration": (1.6, 3.0),
    "pressure": (97.0, 105.0),
    "rpm": (1300.0, 1700.0),
    "power_consumption": (3.6, 5.8),
}
NOISE_SD: dict[str, float] = {
    "temperature": 1.0,
    "vibration": 0.22,
    "pressure": 0.8,
    "rpm": 18.0,
    "power_consumption": 0.15,
}
DEVICE_TYPES = ("centrifugal_pump", "compressor", "conveyor_motor", "cooling_fan")


@dataclass
class SensorProfile:
    device_id: str
    plant_id: str
    device_type: str
    baselines: dict[str, float]
    noise: dict[str, float] = field(default_factory=lambda: dict(NOISE_SD))


def build_fleet(n_devices: int, rng: random.Random, plants: int | None = None) -> list[SensorProfile]:
    """Create ``n_devices`` profiles spread over a handful of plants."""
    plants = plants or max(1, min(5, (n_devices + 9) // 10))
    fleet = []
    for i in range(n_devices):
        baselines = {m: rng.uniform(lo, hi) for m, (lo, hi) in BASELINE_RANGES.items()}
        fleet.append(
            SensorProfile(
                device_id=f"sensor_{i + 1:03d}",
                plant_id=f"plant_{(i % plants) + 1:02d}",
                device_type=DEVICE_TYPES[i % len(DEVICE_TYPES)],
                baselines=baselines,
            )
        )
    return fleet


class SimulatedDevice:
    """Produces one set of measurements per call to :meth:`read`."""

    def __init__(self, profile: SensorProfile, seed: int):
        self.profile = profile
        self.rng = random.Random(seed)

    def read(self, offsets: dict[str, float], noise_factor: dict[str, float]) -> dict[str, float]:
        p = self.profile
        b = p.baselines

        def noisy(metric: str) -> float:
            return self.rng.gauss(0.0, p.noise[metric] * noise_factor.get(metric, 1.0))

        rpm_dev = noisy("rpm")
        rpm = b["rpm"] + rpm_dev + offsets.get("rpm", 0.0)
        # Coupling: +100 rpm -> +0.15 kW and +0.4 degC (small compared with the noise).
        power = b["power_consumption"] + 0.0015 * rpm_dev + noisy("power_consumption")
        temperature = b["temperature"] + 0.004 * rpm_dev + noisy("temperature")
        vibration = b["vibration"] + noisy("vibration")
        pressure = b["pressure"] + noisy("pressure")

        values = {
            "temperature": temperature + offsets.get("temperature", 0.0),
            "vibration": max(0.0, vibration + offsets.get("vibration", 0.0)),
            "pressure": max(0.0, pressure + offsets.get("pressure", 0.0)),
            "rpm": max(0.0, rpm),
            "power_consumption": max(0.0, power + offsets.get("power_consumption", 0.0)),
        }
        precision = {"temperature": 2, "vibration": 3, "pressure": 2, "rpm": 1, "power_consumption": 3}
        return {m: round(v, precision[m]) for m, v in values.items()}
