"""Reproducible anomaly and data-quality fault injection.

Anomalies are scheduled per device as a Poisson process (``anomaly_rate`` episodes per
device-hour). Every episode is recorded as ground truth so detectors can be scored for
precision, recall and detection delay. All randomness comes from a seeded RNG, so the
same seed always yields the same episodes.
"""

from __future__ import annotations

import random
from dataclasses import asdict, dataclass

# shape: point | step | ramp | degradation
#   duration: seconds the disturbance is active (ramps: time to reach full magnitude)
#   hold:     ramps only, seconds the device stays at the new level before "maintenance"
ANOMALY_TYPES: dict[str, dict] = {
    "temperature_spike": {"metric": "temperature", "shape": "point", "duration": (0, 0),
                          "magnitude": (16.0, 28.0), "weight": 0.25, "category": "sudden"},
    "vibration_burst": {"metric": "vibration", "shape": "step", "duration": (3, 8),
                        "magnitude": (2.2, 4.0), "weight": 0.17, "category": "sudden"},
    "pressure_drop": {"metric": "pressure", "shape": "step", "duration": (20, 60),
                      "magnitude": (-14.0, -8.0), "weight": 0.14, "category": "sudden"},
    "power_surge": {"metric": "power_consumption", "shape": "step", "duration": (10, 30),
                    "magnitude": (2.0, 3.5), "weight": 0.14, "category": "sudden"},
    "temperature_drift": {"metric": "temperature", "shape": "ramp", "duration": (900, 1800),
                          "hold": (600, 1200), "magnitude": (7.0, 13.0), "weight": 0.15,
                          "category": "gradual"},
    "vibration_degradation": {"metric": "vibration", "shape": "degradation", "duration": (900, 1800),
                              "hold": (600, 1200), "magnitude": (1.0, 2.0), "noise_factor": 2.5,
                              "weight": 0.15, "category": "gradual"},
}


@dataclass
class AnomalyEpisode:
    episode_id: str
    device_id: str
    metric: str
    anomaly_type: str
    category: str
    shape: str
    start: float          # epoch seconds (event time)
    ramp: float           # seconds to full magnitude (0 for non-ramps)
    end: float            # epoch seconds, exclusive
    magnitude: float
    noise_factor: float = 1.0

    def active(self, t: float) -> bool:
        return self.start <= t < self.end

    def offset(self, t: float) -> float:
        if not self.active(t):
            return 0.0
        if self.shape in ("ramp", "degradation") and self.ramp > 0:
            return self.magnitude * min(1.0, (t - self.start) / self.ramp)
        return self.magnitude

    def noise_multiplier(self, t: float) -> float:
        if self.shape != "degradation" or not self.active(t):
            return 1.0
        progress = min(1.0, (t - self.start) / self.ramp) if self.ramp else 1.0
        return 1.0 + (self.noise_factor - 1.0) * progress

    def to_record(self) -> dict:
        return asdict(self)


class AnomalyInjector:
    def __init__(self, rng: random.Random, anomaly_rate: float, intensity: float, interval: float):
        self.rng = rng
        self.anomaly_rate = anomaly_rate      # episodes per device-hour
        self.intensity = intensity
        self.interval = interval              # seconds between two samples of one device
        self.active: dict[str, AnomalyEpisode] = {}
        self._counter = 0
        names = list(ANOMALY_TYPES)
        self._names = names
        self._weights = [ANOMALY_TYPES[n]["weight"] for n in names]

    def maybe_start(self, device_id: str, t: float) -> AnomalyEpisode | None:
        """Possibly start a new episode for ``device_id`` at event time ``t``."""
        current = self.active.get(device_id)
        if current is not None:
            if t < current.end:
                return None
            del self.active[device_id]
        p = self.anomaly_rate / 3600.0 * self.interval
        if self.rng.random() >= p:
            return None

        name = self.rng.choices(self._names, weights=self._weights, k=1)[0]
        spec = ANOMALY_TYPES[name]
        magnitude = self.rng.uniform(*spec["magnitude"]) * self.intensity
        shape = spec["shape"]
        if shape == "point":
            ramp, end = 0.0, t + self.interval * 0.5
        elif shape == "step":
            ramp, end = 0.0, t + self.rng.uniform(*spec["duration"])
        else:
            ramp = self.rng.uniform(*spec["duration"])
            end = t + ramp + self.rng.uniform(*spec["hold"])
        self._counter += 1
        episode = AnomalyEpisode(
            episode_id=f"ep-{device_id}-{self._counter:05d}",
            device_id=device_id,
            metric=spec["metric"],
            anomaly_type=name,
            category=spec["category"],
            shape=shape,
            start=t,
            ramp=ramp,
            end=end,
            magnitude=round(magnitude, 3),
            noise_factor=spec.get("noise_factor", 1.0),
        )
        self.active[device_id] = episode
        return episode

    def effects(self, device_id: str, t: float) -> tuple[dict[str, float], dict[str, float]]:
        """Return (offsets, noise multipliers) for a device at time ``t``."""
        ep = self.active.get(device_id)
        if ep is None or not ep.active(t):
            return {}, {}
        return {ep.metric: ep.offset(t)}, {ep.metric: ep.noise_multiplier(t)}


@dataclass
class DataQualityConfig:
    enabled: bool = True
    missing_prob: float = 0.01      # one measurement set to null
    duplicate_prob: float = 0.005   # event re-sent with the same event_id
    delay_prob: float = 0.01        # event held back and delivered late (out of order)
    delay_range: tuple[float, float] = (5.0, 200.0)  # some exceed the 2 min watermark on purpose
    malformed_prob: float = 0.002   # record that must be rejected by the validator


MALFORMED_KINDS = ("bad_json", "missing_device", "string_measurement", "negative_rpm", "bad_timestamp")


def corrupt(event: dict, kind: str) -> str:
    """Serialize ``event`` in a deliberately broken way."""
    import json

    broken = dict(event)
    if kind == "bad_json":
        return json.dumps(broken)[:-7]  # truncated payload
    if kind == "missing_device":
        broken.pop("device_id", None)
    elif kind == "string_measurement":
        broken["temperature"] = "ERR_SENSOR"
    elif kind == "negative_rpm":
        broken["rpm"] = -250.0
    elif kind == "bad_timestamp":
        broken["event_time"] = "31/02/2026 25:61"
    return json.dumps(broken)
