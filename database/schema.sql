-- StreamPulse Engine schema (PostgreSQL 16).
--
-- Every table written by Spark has a natural or deterministic key so that replays after
-- a failure are idempotent (ON CONFLICT ... DO NOTHING / DO UPDATE). This is what gives
-- the pipeline "effectively-once" persistence on top of at-least-once delivery.

CREATE TABLE IF NOT EXISTS devices (
    device_id           TEXT PRIMARY KEY,
    plant_id            TEXT NOT NULL,
    device_type         TEXT NOT NULL DEFAULT 'industrial_pump',
    registration_date   TIMESTAMPTZ NOT NULL DEFAULT now(),
    operational_status  TEXT NOT NULL DEFAULT 'running',
    last_seen_at        TIMESTAMPTZ
);

-- Sliding-window aggregates produced by the Spark aggregator (1 min windows / 15 s slide).
CREATE TABLE IF NOT EXISTS telemetry_metrics (
    id                       BIGSERIAL PRIMARY KEY,
    device_id                TEXT NOT NULL,
    plant_id                 TEXT,
    window_start             TIMESTAMPTZ NOT NULL,
    window_end               TIMESTAMPTZ NOT NULL,
    event_time               TIMESTAMPTZ NOT NULL,      -- = window_end, convenient for charts
    avg_temperature          DOUBLE PRECISION,
    min_temperature          DOUBLE PRECISION,
    max_temperature          DOUBLE PRECISION,
    std_temperature          DOUBLE PRECISION,
    avg_vibration            DOUBLE PRECISION,
    max_vibration            DOUBLE PRECISION,
    std_vibration            DOUBLE PRECISION,
    avg_pressure             DOUBLE PRECISION,
    min_pressure             DOUBLE PRECISION,
    avg_rpm                  DOUBLE PRECISION,
    avg_power_consumption    DOUBLE PRECISION,
    event_count              INTEGER NOT NULL,
    missing_measurement_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT telemetry_metrics_window_uk UNIQUE (device_id, window_start, window_end)
);
CREATE INDEX IF NOT EXISTS telemetry_metrics_device_time_idx
    ON telemetry_metrics (device_id, window_end DESC);

-- Detection results that survived alert de-duplication / cooldown.
CREATE TABLE IF NOT EXISTS alerts (
    alert_id          TEXT PRIMARY KEY,               -- sha1(device|metric|algorithm|event_id)
    device_id         TEXT NOT NULL,
    plant_id          TEXT,
    metric_name       TEXT NOT NULL,
    algorithm         TEXT NOT NULL,                  -- threshold | zscore | cusum | drift
    alert_kind        TEXT NOT NULL,                  -- point_anomaly | persistent_shift | concept_drift
    observed_value    DOUBLE PRECISION,
    baseline_value    DOUBLE PRECISION,
    deviation_score   DOUBLE PRECISION,
    severity          TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    description       TEXT NOT NULL,
    event_id          TEXT NOT NULL,                  -- event that triggered the detection
    detected_at       TIMESTAMPTZ NOT NULL,           -- event time of the triggering event
    persisted_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    status            TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acknowledged', 'resolved')),
    -- A statistical alert is evidence of change, not proof of failure. Only an operator
    -- can confirm (or reject) an operational failure when resolving the alert.
    confirmed_failure BOOLEAN,
    acknowledged_at   TIMESTAMPTZ,
    acknowledged_by   TEXT,
    resolved_at       TIMESTAMPTZ,
    resolution_note   TEXT
);
CREATE INDEX IF NOT EXISTS alerts_detected_idx ON alerts (detected_at DESC);
CREATE INDEX IF NOT EXISTS alerts_device_idx ON alerts (device_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS alerts_status_idx ON alerts (status);

-- One row per micro-batch per streaming query.
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id                     BIGSERIAL PRIMARY KEY,
    recorded_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    query_name             TEXT NOT NULL,
    batch_id               BIGINT NOT NULL,
    events_processed       BIGINT NOT NULL DEFAULT 0,
    events_rejected        BIGINT NOT NULL DEFAULT 0,
    alerts_generated       BIGINT NOT NULL DEFAULT 0,
    active_devices         INTEGER NOT NULL DEFAULT 0,
    processing_latency_ms  DOUBLE PRECISION,   -- avg(batch time - Kafka ingest time)
    end_to_end_latency_ms  DOUBLE PRECISION,   -- avg(batch time - event (sensor) time)
    input_rows_per_sec     DOUBLE PRECISION,   -- filled by the StreamingQueryListener
    processed_rows_per_sec DOUBLE PRECISION,
    batch_duration_ms      BIGINT,
    CONSTRAINT pipeline_metrics_batch_uk UNIQUE (query_name, batch_id)
);
CREATE INDEX IF NOT EXISTS pipeline_metrics_time_idx ON pipeline_metrics (recorded_at DESC);

-- Dead-letter store for rejected events (they are also published to `invalid-telemetry`).
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id               TEXT PRIMARY KEY,                -- topic-partition-offset
    received_at      TIMESTAMPTZ NOT NULL,
    reason           TEXT NOT NULL,
    raw_payload      TEXT,
    kafka_partition  INTEGER,
    kafka_offset     BIGINT
);
CREATE INDEX IF NOT EXISTS dead_letter_received_idx ON dead_letter_events (received_at DESC);

-- Ground truth written by the simulator for every injected anomaly episode.
CREATE TABLE IF NOT EXISTS injected_anomalies (
    episode_id    TEXT PRIMARY KEY,
    device_id     TEXT NOT NULL,
    metric_name   TEXT NOT NULL,
    anomaly_type  TEXT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    ended_at      TIMESTAMPTZ NOT NULL,
    magnitude     DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS injected_anomalies_device_idx ON injected_anomalies (device_id, started_at);

-- Single-row control plane between the API and the simulator.
CREATE TABLE IF NOT EXISTS simulation_control (
    id              SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    running         BOOLEAN NOT NULL DEFAULT false,
    devices         INTEGER NOT NULL DEFAULT 20,
    rate            DOUBLE PRECISION NOT NULL DEFAULT 100,
    seed            INTEGER NOT NULL DEFAULT 42,
    anomaly_rate    DOUBLE PRECISION NOT NULL DEFAULT 2.0,   -- episodes per device-hour
    intensity       DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    data_quality    BOOLEAN NOT NULL DEFAULT true,           -- inject duplicates / delays / malformed
    config_version  INTEGER NOT NULL DEFAULT 1,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- heartbeat written by the simulator
    producer_heartbeat_at TIMESTAMPTZ,
    events_sent           BIGINT NOT NULL DEFAULT 0,
    producer_rate         DOUBLE PRECISION
);
INSERT INTO simulation_control (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
