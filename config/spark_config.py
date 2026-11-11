"""Spark Structured Streaming settings.

Window and watermark defaults follow the design document: 1 minute windows sliding
every 15 seconds, with a 2 minute watermark for late events.
"""

from __future__ import annotations

from dataclasses import dataclass

from config import env, env_int


@dataclass(frozen=True)
class SparkConfig:
    app_name: str = env("SPARK_APP_NAME", "streampulse-engine")
    master: str = env("SPARK_MASTER_URL", "local[*]")
    checkpoint_root: str = env("SPARK_CHECKPOINT_ROOT", "/opt/streampulse/checkpoints")
    shuffle_partitions: int = env_int("SPARK_SHUFFLE_PARTITIONS", 4)
    state_store: str = env("STATE_STORE_PROVIDER", "hdfs")   # hdfs | rocksdb

    window_duration: str = env("WINDOW_DURATION", "1 minute")
    slide_duration: str = env("WINDOW_SLIDE", "15 seconds")
    watermark_delay: str = env("WATERMARK_DELAY", "2 minutes")

    validator_trigger: str = env("VALIDATOR_TRIGGER", "2 seconds")
    aggregator_trigger: str = env("AGGREGATOR_TRIGGER", "10 seconds")
    detector_trigger: str = env("DETECTOR_TRIGGER", "5 seconds")

    max_offsets_per_trigger: int = env_int("MAX_OFFSETS_PER_TRIGGER", 200000)
    starting_offsets: str = env("KAFKA_STARTING_OFFSETS", "earliest")

    def checkpoint(self, query_name: str) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{query_name}"


SPARK = SparkConfig()
