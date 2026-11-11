# Product

<!-- impeccable:product-schema 1 -->

> Interview substitution: the owner asked for this project to be built unattended with no
> questions. Every fact below comes from the written project brief or the code in this
> repository. Facts marked *(inferred)* are assumptions to confirm.

## Platform

web

## Stack

delegated: static HTML + CSS + vanilla JavaScript served by nginx, no build step, data from
the FastAPI service under `/api`. Chosen so the dashboard stays a thin, dependency-free
client of the API and the repository's focus stays on the streaming backend (brief: "A
simple frontend is sufficient").

## Users

- Data / platform engineers operating the pipeline: they watch throughput, latency, batch
  health and rejected records while the system runs.
- Reliability / maintenance engineers triaging alerts: they decide whether a statistical
  alert is a real machine problem, then acknowledge or resolve it. *(inferred from the alert
  lifecycle in the brief)*
- Portfolio reviewers (hiring managers, interviewers) who start the stack with one command
  and need to understand within a minute what it does. *(inferred: the project's stated
  purpose is a data-engineering portfolio)*

## Product Purpose

StreamPulse Engine ingests simulated industrial IoT telemetry through Kafka, processes it
with PySpark Structured Streaming (validation, event-time windows, watermarking,
deduplication), runs four detectors (fixed threshold, z-score, CUSUM, two-window drift) on
the same stream, and persists metrics and alerts to PostgreSQL. Success means the
operator can see at a glance that the pipeline is healthy, which devices are misbehaving,
and why each alert fired.

## Positioning

Detection is explained, not just flagged. Every alert carries the algorithm, the
observed value against its learned baseline, a deviation score and a sentence of
reasoning. Alerts are explicitly *statistical evidence*, separate from *confirmed
failures*, which only an operator can record. Ground truth from the simulator lets the
system score its own detectors live (precision / recall).

## Operating Context

- Runs locally via `docker compose up`; dashboard at `localhost:8080`, API at `:8000`.
- Data refreshes every few seconds (micro-batches of 2-10 s).
- Fleet of ~20 simulated devices across a few plants by default; configurable up to hundreds.
- Detector vocabulary: point anomaly, persistent shift, concept drift; severities low,
  medium, high, critical; alert states open, acknowledged, resolved.

## Capabilities and Constraints

- API endpoints exist for pipeline status/metrics, dead letters, devices and their window
  history, alerts (filter, summary, live evaluation, acknowledge, resolve) and simulator
  start/stop/configure.
- The API reads only from PostgreSQL; there is no websocket, so the dashboard polls.
- All telemetry is synthetic (simulated devices). Nothing may present it as real plant data.

## Evidence on Hand

- Live data from the running stack (synthetic telemetry, seeded).
- Offline detector benchmark: `benchmarks/results/detection_evaluation.md`.
- No logo, brand assets, customers or testimonials exist; none may be invented.

## Product Principles

1. Explain every signal: an alert without its baseline and reasoning is noise.
2. Evidence is not verdict: statistical alerts and confirmed failures stay distinct.
3. Health first: pipeline liveness is answerable in one glance before any detail.
4. Honest numbers: show measured values, label synthetic data, never imply guarantees.

## Accessibility & Inclusion

WCAG 2.1 AA contrast and keyboard operability for all controls *(inferred default)*.
Severity must never be encoded by colour alone.
