"""Alert de-duplication, cooldown and identity.

A machine that keeps breaching the same limit must not produce an alert per second.
For every (metric, algorithm) of a device the manager remembers the last emitted alert
(in *event time*, so replays after a failure make identical decisions) and suppresses
repeats inside the configured cooldown, unless the new detection is more severe.

``alert_id`` is a deterministic hash of (device, metric, algorithm, triggering event):
re-processing the same events after a crash yields the same ids, and the
``ON CONFLICT (alert_id) DO NOTHING`` insert makes persistence idempotent.
"""

from __future__ import annotations

import hashlib

from detection.base_detector import SEVERITY_RANK, DetectionResult


def make_alert_id(device_id: str, metric: str, algorithm: str, event_id: str) -> str:
    raw = f"{device_id}|{metric}|{algorithm}|{event_id}".encode()
    return "al_" + hashlib.sha1(raw).hexdigest()[:20]


class AlertManager:
    def __init__(self, cooldown_seconds: dict[str, float]):
        self.cooldown = cooldown_seconds

    @staticmethod
    def init_state() -> dict:
        return {}

    def admit(self, state: dict, result: DetectionResult) -> tuple[bool, int]:
        """Decide whether ``result`` becomes an alert.

        Returns ``(emit, suppressed_since_last)``.
        """
        key = f"{result.metric}|{result.algorithm}"
        entry = state.get(key)
        rank = SEVERITY_RANK[result.severity]
        cooldown = float(self.cooldown.get(result.algorithm, 60))
        if entry is not None:
            within = abs(result.detected_at - entry["ts"]) < cooldown
            if within and rank <= entry["rank"]:
                entry["suppressed"] += 1
                return False, 0
        suppressed = entry["suppressed"] if entry else 0
        state[key] = {"ts": result.detected_at, "rank": rank, "suppressed": 0}
        return True, suppressed
