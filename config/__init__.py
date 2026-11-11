"""Shared configuration for every StreamPulse service.

All runtime settings come from environment variables (see ``.env.example``) so the
same code runs inside Docker Compose, in CI and on a developer laptop.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

METRICS: tuple[str, ...] = ("temperature", "vibration", "pressure", "rpm", "power_consumption")


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class PostgresConfig:
    host: str = env("POSTGRES_HOST", "localhost")
    port: int = env_int("POSTGRES_PORT", 5432)
    database: str = env("POSTGRES_DB", "streampulse")
    user: str = env("POSTGRES_USER", "streampulse")
    password: str = env("POSTGRES_PASSWORD", "streampulse")

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


def postgres_config() -> PostgresConfig:
    """Build the config at call time so tests can override environment variables."""
    return PostgresConfig(
        host=env("POSTGRES_HOST", "localhost"),
        port=env_int("POSTGRES_PORT", 5432),
        database=env("POSTGRES_DB", "streampulse"),
        user=env("POSTGRES_USER", "streampulse"),
        password=env("POSTGRES_PASSWORD", "streampulse"),
    )
