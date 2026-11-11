"""Kafka topology and client settings."""

from __future__ import annotations

from dataclasses import dataclass, field

from config import env, env_int


@dataclass(frozen=True)
class Topics:
    raw: str = env("TOPIC_RAW", "raw-telemetry")
    validated: str = env("TOPIC_VALIDATED", "validated-telemetry")
    invalid: str = env("TOPIC_INVALID", "invalid-telemetry")
    anomalies: str = env("TOPIC_ANOMALIES", "anomaly-events")

    def all(self) -> list[str]:
        return [self.raw, self.validated, self.invalid, self.anomalies]


@dataclass(frozen=True)
class KafkaConfig:
    # Inside the compose network the broker is kafka:9092; from the host it is localhost:29092.
    bootstrap_servers: str = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    partitions: int = env_int("KAFKA_PARTITIONS", 6)
    topics: Topics = field(default_factory=Topics)

    def producer_settings(self) -> dict:
        """librdkafka settings for the telemetry producer.

        ``acks=all`` + idempotence means a broker-acknowledged write is never duplicated by
        producer retries. Duplicates that still reach Kafka are the *simulated* ones, which
        Spark removes with ``dropDuplicatesWithinWatermark``.
        """
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 10,
            "batch.num.messages": 10000,
            "compression.type": "lz4",
            "client.id": "streampulse-simulator",
        }


KAFKA = KafkaConfig()
