-- OPTIONAL: convert telemetry_metrics into a TimescaleDB hypertable.
--
-- Not applied by default (the stack uses plain PostgreSQL 16). To try it, swap the
-- postgres image for `timescale/timescaledb:latest-pg16` and run:
--
--   docker compose exec postgres psql -U streampulse -d streampulse \
--       -f /docker-entrypoint-initdb.d/migrations/001_optional_timescaledb.sql
--
-- Hypertables require the partitioning column in every unique constraint, so the
-- surrogate primary key is replaced by the natural window key.

CREATE EXTENSION IF NOT EXISTS timescaledb;

ALTER TABLE telemetry_metrics DROP CONSTRAINT IF EXISTS telemetry_metrics_pkey;
ALTER TABLE telemetry_metrics DROP CONSTRAINT IF EXISTS telemetry_metrics_window_uk;
ALTER TABLE telemetry_metrics
    ADD CONSTRAINT telemetry_metrics_window_uk UNIQUE (device_id, window_start, window_end);

SELECT create_hypertable('telemetry_metrics', 'window_start',
                         chunk_time_interval => INTERVAL '1 day',
                         migrate_data => true,
                         if_not_exists => true);

-- Keep 30 days of window aggregates.
SELECT add_retention_policy('telemetry_metrics', INTERVAL '30 days', if_not_exists => true);
