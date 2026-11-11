---
version: 1
slug: "dashboard-index-html"
primary_target: "dashboard/index.html"
related_targets: []
---

# Surface brief: monitoring dashboard

Scope: `dashboard/` (static HTML/CSS/JS served by nginx, data from `/api`). Mode: Operate.

Audience and task: pipeline operators and reliability engineers check health at a glance,
watch throughput / latency, inspect a device's telemetry around an anomaly, and
acknowledge or resolve alerts (confirming or rejecting a real failure). Portfolio
reviewers must understand the system within a minute.

Unattended run: the owner asked for no questions; the decision page was not served and the
assigned direction was built. Challenger verdicts: zoo guide map declined (kept: flat,
unmodulated colour fields); Crouwel grid specimen declined (kept: the grid stays visible);
botanical folio declined (kept: identical scale across plates); timetable slide rack
declined (kept: rank by weight and case, few sizes); design-annual plate section declined
(kept: hairline-only structure); exposure-record sheets competitive (kept: severity as
mark density as well as colour).

## Direction contract

THESIS: The dashboard reads like a well-kept engineering drawing set: every signal is
dimensioned and referenced. It refuses the default grey card grid with glowing neon
charts and a hero-metric row.

OWN-WORLD: Cool drafting-film ground (#EEF1EF), whiter sheets, graphite ink and a graphite
rail, 1px hairline rules as the only structure (no shadows, no rounded cards), diazo
violet-blue as the data and selection colour, redline red only for critical marks and
errors, amber for degraded, green for healthy. One grotesk family (Barlow) with semi-condensed
caps for labels and tabular figures for every number. Severity marks use fill density
(outline, hatched, cross-hatched, solid) plus a text label.

STORY: The visitor sees whether the pipeline is healthy, how fast data flows, which devices
are misbehaving and why each alert fired. They then acknowledge or resolve it, and
statistical evidence stays separate from a confirmed failure.

FIRST VIEWPORT: Graphite left rail (product name, five views, live status line). Overview
view: a ruled header row of pipeline facts (status, ingest rate, latency, devices, alerts)
set as title-block cells; below, the throughput chart spanning two-thirds with
a query health table to its right; then the latest alerts table with balloon numbers.

FORM: Grounded list position 4 of 7 (engineering drawing conventions). seed:bf7ce89890690083.
Signature move: alerts are numbered drafting balloons on the telemetry charts, each with a
leader line to its onset point and cross-referenced to the same number in the alert
table; hovering either highlights both.

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance
