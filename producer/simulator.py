"""Telemetry simulator.

Two ways to use it:

* :class:`TelemetrySimulator` is a deterministic, clock-agnostic generator. The offline
  benchmarks drive it with a virtual clock (hours of telemetry in seconds).
* ``python -m producer.simulator`` runs the live producer: it paces the simulator on the
  wall clock, publishes to Kafka, records ground truth in PostgreSQL, and obeys the
  ``simulation_control`` row that the API's ``/simulation/start|stop`` endpoints update.
"""

from __future__ import annotations

import heapq
import json
import logging
import os
import random
import signal
import time
from collections.abc import Iterator
from dataclasses import dataclass, field

from producer.anomaly_injector import (
    MALFORMED_KINDS,
    AnomalyEpisode,
    AnomalyInjector,
    DataQualityConfig,
    corrupt,
)
from producer.event_schema import SCHEMA_VERSION, format_event_time
from producer.sensor import SensorProfile, SimulatedDevice, build_fleet

log = logging.getLogger("streampulse.simulator")


@dataclass
class SimulationConfig:
    devices: int = 20
    rate: float = 100.0            # total events per second across the fleet
    seed: int = 42
    anomaly_rate: float = 2.0      # anomaly episodes per device-hour
    intensity: float = 1.0         # multiplier on anomaly magnitudes
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)
    run_id: str = "r0"             # namespaces event ids so restarts never reuse ids

    @property
    def interval(self) -> float:
        """Seconds between two consecutive samples of the same device."""
        return self.devices / self.rate


@dataclass
class Outgoing:
    """A message ready for Kafka. ``event`` is None for malformed payloads."""

    key: str
    value: str
    event: dict | None
    event_time: float
    kind: str = "normal"   # normal | duplicate | delayed | malformed


class TelemetrySimulator:
    def __init__(self, config: SimulationConfig, start_time: float):
        self.config = config
        self.rng = random.Random(config.seed)
        self.fleet: list[SensorProfile] = build_fleet(config.devices, self.rng)
        self.devices = [SimulatedDevice(p, seed=config.seed * 7919 + i) for i, p in enumerate(self.fleet)]
        self.injector = AnomalyInjector(self.rng, config.anomaly_rate, config.intensity, config.interval)
        self.dq = config.data_quality
        interval = config.interval
        # Stagger devices so the fleet emits at a smooth rate instead of in bursts.
        self._next_emit = [start_time + interval * i / config.devices for i in range(config.devices)]
        self._seq = [0] * config.devices
        self._delayed: list[tuple[float, int, Outgoing]] = []
        self._tiebreak = 0
        self.new_episodes: list[AnomalyEpisode] = []
        self.episodes: list[AnomalyEpisode] = []

    # ------------------------------------------------------------------ generation
    def _make_event(self, idx: int, t: float) -> tuple[dict, AnomalyEpisode | None]:
        profile = self.fleet[idx]
        episode = self.injector.maybe_start(profile.device_id, t)
        if episode is not None:
            self.new_episodes.append(episode)
            self.episodes.append(episode)
        offsets, noise = self.injector.effects(profile.device_id, t)
        values = self.devices[idx].read(offsets, noise)
        self._seq[idx] += 1
        event = {
            "event_id": f"evt-{self.config.run_id}-{profile.device_id}-{self._seq[idx]:08d}",
            "device_id": profile.device_id,
            "plant_id": profile.plant_id,
            "event_time": format_event_time(t),
            **values,
            "status": "running",
            "schema_version": SCHEMA_VERSION,
        }
        return event, self.injector.active.get(profile.device_id)

    def _apply_data_quality(self, event: dict, t: float, now: float) -> list[Outgoing]:
        key = event["device_id"]
        if not self.dq.enabled:
            return [Outgoing(key, json.dumps(event), event, t)]
        r = self.rng.random()
        dq = self.dq
        if r < dq.malformed_prob:
            kind = self.rng.choice(MALFORMED_KINDS)
            return [Outgoing(key, corrupt(event, kind), None, t, "malformed")]
        if self.rng.random() < dq.missing_prob:
            event = dict(event)
            event[self.rng.choice(["temperature", "vibration", "pressure", "rpm", "power_consumption"])] = None
        out = Outgoing(key, json.dumps(event), event, t)
        if self.rng.random() < dq.delay_prob:
            release = now + self.rng.uniform(*dq.delay_range)
            out.kind = "delayed"
            self._tiebreak += 1
            heapq.heappush(self._delayed, (release, self._tiebreak, out))
            return []
        messages = [out]
        if self.rng.random() < dq.duplicate_prob:
            messages.append(Outgoing(key, out.value, event, t, "duplicate"))
        return messages

    def step(self, now: float) -> list[Outgoing]:
        """Emit every message due before ``now`` (event time = scheduled time)."""
        out: list[Outgoing] = []
        interval = self.config.interval
        for idx in range(len(self.fleet)):
            while self._next_emit[idx] < now:
                t = self._next_emit[idx]
                self._next_emit[idx] = t + interval
                event, _ = self._make_event(idx, t)
                out.extend(self._apply_data_quality(event, t, now))
        while self._delayed and self._delayed[0][0] <= now:
            out.append(heapq.heappop(self._delayed)[2])
        return out

    def drain_new_episodes(self) -> list[AnomalyEpisode]:
        episodes, self.new_episodes = self.new_episodes, []
        return episodes

    def generate(self, start_time: float, duration_s: float, step_s: float = 1.0) -> Iterator[Outgoing]:
        """Virtual-clock generation for benchmarks and tests."""
        t = start_time
        end = start_time + duration_s
        while t < end:
            t = min(end, t + step_s)
            yield from self.step(t)


# ====================================================================== live producer
class LiveProducer:
    """Paces the simulator on the wall clock and publishes to Kafka."""

    POLL_CONTROL_S = 1.0
    HEARTBEAT_S = 5.0

    def __init__(self) -> None:
        from confluent_kafka import Producer

        from config.kafka_config import KAFKA

        self.kafka_cfg = KAFKA
        self.producer = Producer(KAFKA.producer_settings())
        self.topic = KAFKA.topics.raw
        self.conn = _connect_postgres()
        self.sim: TelemetrySimulator | None = None
        self.config_version: int | None = None
        self.running = False
        self.stop_requested = False
        self.sent = 0
        self.failed = 0
        self._sent_window: list[tuple[float, int]] = []

    # -- control plane -------------------------------------------------------------
    def bootstrap_control(self) -> None:
        if os.environ.get("SIM_AUTOSTART", "true").lower() != "true":
            return
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE simulation_control
                   SET running = true, devices = %s, rate = %s, seed = %s, anomaly_rate = %s,
                       intensity = %s, config_version = config_version + 1, updated_at = now()
                 WHERE id = 1
                """,
                (
                    int(os.environ.get("SIM_DEVICES", 20)),
                    float(os.environ.get("SIM_RATE", 100)),
                    int(os.environ.get("SIM_SEED", 42)),
                    float(os.environ.get("SIM_ANOMALY_RATE", 2.0)),
                    float(os.environ.get("SIM_INTENSITY", 1.0)),
                ),
            )
        self.conn.commit()

    def read_control(self) -> dict:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT running, devices, rate, seed, anomaly_rate, intensity, data_quality, config_version "
                "FROM simulation_control WHERE id = 1"
            )
            row = cur.fetchone()
        self.conn.commit()
        keys = ("running", "devices", "rate", "seed", "anomaly_rate", "intensity", "data_quality", "config_version")
        return dict(zip(keys, row, strict=False))

    def apply_control(self, control: dict) -> None:
        self.running = bool(control["running"])
        if control["config_version"] != self.config_version or self.sim is None:
            cfg = SimulationConfig(
                devices=int(control["devices"]),
                rate=float(control["rate"]),
                seed=int(control["seed"]),
                anomaly_rate=float(control["anomaly_rate"]),
                intensity=float(control["intensity"]),
                data_quality=DataQualityConfig(enabled=bool(control["data_quality"])),
                run_id=f"{int(time.time()) % 10_000_000:07d}",
            )
            self.sim = TelemetrySimulator(cfg, start_time=time.time())
            self.config_version = control["config_version"]
            self.register_devices(self.sim.fleet)
            log.info("simulation configured: %s devices, %.0f ev/s, seed=%s, run=%s",
                     cfg.devices, cfg.rate, cfg.seed, cfg.run_id)

    def register_devices(self, fleet: list[SensorProfile]) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO devices (device_id, plant_id, device_type, operational_status)
                VALUES (%s, %s, %s, 'running')
                ON CONFLICT (device_id) DO UPDATE
                   SET plant_id = EXCLUDED.plant_id, device_type = EXCLUDED.device_type
                """,
                [(p.device_id, p.plant_id, p.device_type) for p in fleet],
            )
        self.conn.commit()

    def record_episodes(self, episodes: list[AnomalyEpisode]) -> None:
        if not episodes:
            return
        from datetime import datetime, timezone

        rows = [
            (
                f"{self.sim.config.run_id}-{e.episode_id}",
                e.device_id,
                e.metric,
                e.anomaly_type,
                datetime.fromtimestamp(e.start, tz=timezone.utc),
                datetime.fromtimestamp(e.end, tz=timezone.utc),
                e.magnitude,
            )
            for e in episodes
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO injected_anomalies (episode_id, device_id, metric_name, anomaly_type, "
                "started_at, ended_at, magnitude) VALUES (%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (episode_id) DO NOTHING",
                rows,
            )
        self.conn.commit()

    def heartbeat(self) -> None:
        now = time.time()
        self._sent_window = [(t, n) for t, n in self._sent_window if now - t <= 10.0]
        rate = sum(n for _, n in self._sent_window) / 10.0
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE simulation_control SET producer_heartbeat_at = now(), events_sent = %s, "
                "producer_rate = %s WHERE id = 1",
                (self.sent, rate if self.running else 0.0),
            )
        self.conn.commit()

    # -- data plane ------------------------------------------------------------------
    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.failed += 1
            if self.failed % 100 == 1:
                log.warning("delivery failed (%s total): %s", self.failed, err)

    def publish(self, messages: list[Outgoing]) -> None:
        for m in messages:
            while True:
                try:
                    self.producer.produce(self.topic, key=m.key.encode(), value=m.value.encode(),
                                          on_delivery=self._on_delivery)
                    break
                except BufferError:
                    self.producer.poll(0.2)   # local queue full: let librdkafka drain it
        self.producer.poll(0)
        self.sent += len(messages)
        self._sent_window.append((time.time(), len(messages)))

    def run(self) -> None:
        self.bootstrap_control()
        last_control = last_beat = 0.0
        while not self.stop_requested:
            now = time.time()
            if now - last_control >= self.POLL_CONTROL_S:
                try:
                    self.apply_control(self.read_control())
                except Exception:  # noqa: BLE001 - keep producing through DB hiccups
                    log.exception("control-plane read failed; reconnecting")
                    self.conn = _connect_postgres()
                last_control = now
            if self.running and self.sim is not None:
                self.publish(self.sim.step(now))
                try:
                    self.record_episodes(self.sim.drain_new_episodes())
                except Exception:  # noqa: BLE001
                    log.exception("could not record ground truth")
                    self.conn = _connect_postgres()
            elif self.sim is not None:
                # Paused: move the schedule forward so resuming doesn't replay a backlog.
                self.sim._next_emit = [max(t, now) for t in self.sim._next_emit]
            if now - last_beat >= self.HEARTBEAT_S:
                try:
                    self.heartbeat()
                except Exception:  # noqa: BLE001
                    self.conn = _connect_postgres()
                last_beat = now
            time.sleep(0.02)
        log.info("flushing producer (%s messages sent)", self.sent)
        self.producer.flush(10)


def _connect_postgres(retries: int = 60):
    import psycopg

    from config import postgres_config

    dsn = postgres_config().dsn
    for attempt in range(1, retries + 1):
        try:
            return psycopg.connect(dsn, connect_timeout=5)
        except psycopg.OperationalError as exc:
            log.warning("postgres not ready (attempt %s): %s", attempt, exc)
            time.sleep(2)
    raise RuntimeError("could not connect to PostgreSQL")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    producer = LiveProducer()

    def _stop(*_):
        producer.stop_requested = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    producer.run()


if __name__ == "__main__":
    main()
