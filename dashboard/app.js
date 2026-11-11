/* StreamPulse dashboard: hash-routed views over the FastAPI JSON API. */
(function () {
  "use strict";
  const API = "/api";
  const POLL_MS = 5000;
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const LABELS = {
    algorithm: { threshold: "Threshold", zscore: "Z-score", cusum: "CUSUM", drift: "Drift" },
    kind: { point_anomaly: "Point anomaly", persistent_shift: "Persistent shift", concept_drift: "Concept drift" },
    metric: { temperature: "Temperature", vibration: "Vibration", pressure: "Pressure", rpm: "RPM", power_consumption: "Power" },
    unit: { temperature: "°C", vibration: "mm/s", pressure: "kPa", rpm: "rpm", power_consumption: "kW" },
    query: { validator: "Validator", aggregator: "Aggregator", detector: "Detector" },
    reason: {
      malformed_json: "Malformed JSON", missing_required_field: "Missing required field",
      invalid_event_time: "Invalid event time", non_numeric_measurement: "Non-numeric measurement",
      out_of_physical_range: "Out of physical range", no_measurements: "No measurements",
    },
    anomaly: {
      temperature_spike: "Temperature spike", vibration_burst: "Vibration burst", pressure_drop: "Pressure drop",
      power_surge: "Power surge", temperature_drift: "Temperature drift", vibration_degradation: "Vibration degradation",
    },
  };
  const SEV_COLOR = { low: "#5d676e", medium: "#a85f00", high: "#b5400c", critical: "#c0291f" };
  const QUERY_COLOR = { validator: "#3a3d9c", aggregator: "#1d7470", detector: "#8a5a2b" };

  const state = {
    view: "overview",
    params: "",
    devices: [],
    simDirty: false,
    alertOffset: 0,
    alertPage: 25,
    timer: null,
    inflight: false,
  };

  // ---------------------------------------------------------------- helpers
  async function api(path, options = {}) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 10000);
    try {
      const res = await fetch(API + path, {
        ...options,
        signal: ctrl.signal,
        headers: options.body ? { "Content-Type": "application/json" } : undefined,
      });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return await res.json();
    } finally {
      clearTimeout(t);
    }
  }

  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const num = (v, d = 0) => (v === null || v === undefined || !isFinite(v) ? "–" : Number(v).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d }));
  const secs = (ms) => (ms === null || ms === undefined || !isFinite(ms) ? "–" : ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(ms < 10000 ? 1 : 0)} s`);
  const pct = (v) => (v === null || v === undefined || !isFinite(v) ? "–" : `${(v * 100).toFixed(1)}%`);
  const ts = (v) => (v ? new Date(v).getTime() : null);

  function ago(v) {
    if (v === null || v === undefined) return "never";
    const s = typeof v === "number" ? v : (Date.now() - ts(v)) / 1000;
    if (s < 1) return "just now";
    if (s < 60) return `${Math.round(s)} s ago`;
    if (s < 3600) return `${Math.round(s / 60)} min ago`;
    if (s < 86400) return `${Math.round(s / 3600)} h ago`;
    return `${Math.round(s / 86400)} d ago`;
  }

  function when(v) {
    const d = new Date(v);
    const p = (n) => String(n).padStart(2, "0");
    const time = `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
    const today = new Date();
    if (d.toDateString() === today.toDateString()) return time;
    return `${d.toLocaleDateString(undefined, { month: "short", day: "numeric" })} ${time}`;
  }

  function sevMark(sev) {
    const fill = { low: "none", medium: "url(#hatch)", high: "url(#crosshatch)", critical: "currentColor" }[sev] || "none";
    return `<span class="sev sev-${esc(sev)}"><svg viewBox="0 0 12 12" aria-hidden="true"><rect x="0.75" y="0.75" width="10.5" height="10.5" fill="${fill}" stroke="currentColor" stroke-width="1.5"/></svg>${esc(sev)}</span>`;
  }
  const statusTag = (s) => `<span class="status-tag status-${esc(s)}">${esc(s)}</span>`;
  const stateTag = (s, label) => `<span class="state state-${esc(s)}">${esc(label || s.replace("_", " "))}</span>`;
  const fmtValue = (v, metric) => (v === null || v === undefined ? "–" : `${num(v, metric === "rpm" ? 0 : 2)} ${LABELS.unit[metric] || ""}`.trim());

  function emptyRow(table, cols, text) {
    $(`#${table} tbody`).innerHTML = `<tr class="empty-row"><td colspan="${cols}">${text}</td></tr>`;
  }

  function bars(target, rows, total) {
    const max = Math.max(1, ...rows.map((r) => r.value));
    target.innerHTML = rows.length
      ? rows.map((r) => `<div class="bar-row"><span>${esc(r.label)}</span><div class="bar-track"><div class="bar-fill" style="width:${(100 * r.value) / max}%;${r.color ? "background:" + r.color : ""}"></div></div><span class="num">${num(r.value)}</span></div>`).join("")
      : `<p class="muted">${total === 0 ? "Nothing recorded in this range." : "No data."}</p>`;
  }

  // ----------------------------------------------------------------- status
  async function refreshStatus() {
    const dot = $("#live-status .dot");
    const text = $("#live-status .live-text");
    try {
      const s = await api("/pipeline/status");
      dot.dataset.state = s.status;
      const stalled = s.queries.filter((q) => q.state !== "running").map((q) => LABELS.query[q.name]);
      const label = { healthy: "Pipeline healthy", degraded: "Pipeline degraded", down: "Pipeline down" }[s.status];
      text.textContent = stalled.length && s.status !== "down" ? `${label}: ${stalled.join(", ")} stalled` : label;
      if (!s.producer.simulation_running && s.status !== "down") text.textContent += " · simulator stopped";
      return s;
    } catch (e) {
      dot.dataset.state = "error";
      text.textContent = "API unreachable, retrying";
      return null;
    }
  }

  // --------------------------------------------------------------- overview
  async function loadOverview() {
    const [status, metrics, alerts, summary] = await Promise.all([
      api("/pipeline/status"),
      api("/pipeline/metrics?minutes=15&bucket_seconds=15"),
      api("/alerts?limit=8"),
      api("/alerts/summary?hours=24"),
    ]);
    const c = metrics.current || {};
    const totals = metrics.totals || {};
    const processed = Number(totals.events_processed || 0);
    const rejected = Number(totals.events_rejected || 0);
    const unresolved = (summary.by_status.open || 0) + (summary.by_status.acknowledged || 0);
    const statusLabel = { healthy: "Healthy", degraded: "Degraded", down: "Down" }[status.status];
    const cells = [
      ["Pipeline", `<span class="status-${status.status}">${statusLabel}</span>`],
      ["Ingest rate", `${num(c.ingest_rate, 0)} <small>events/s</small>`],
      ["Event → detection", secs(c.detection_lag_ms)],
      ["Active devices", num(c.active_devices)],
      ["Events validated", num(processed)],
      ["Rejected", `${num(rejected)} <small>${processed + rejected ? pct(rejected / (processed + rejected)) : ""}</small>`],
      ["Open alerts, 24 h", `${num(unresolved)} <small>${num(summary.by_severity.critical || 0)} critical</small>`],
    ];
    $("#ov-titleblock").innerHTML = cells.map(([k, v]) => `<div><dt>${k}</dt><dd>${v}</dd></div>`).join("");

    // throughput: validated and rejected events per second per bucket
    const bucket = metrics.bucket_seconds;
    const v = metrics.series.filter((r) => r.query_name === "validator");
    const now = Date.now();
    const series = [
      { label: "Validated", color: "#3a3d9c", tipColor: "#a9adf0", points: v.map((r) => [ts(r.bucket), r.events_processed / bucket]) },
      { label: "Rejected", color: "#c0291f", tipColor: "#ef8a80", dash: "4 3", points: v.map((r) => [ts(r.bucket), r.events_rejected / bucket]) },
    ];
    SPCharts.legend($("#ov-throughput-legend"), series);
    SPCharts.line($("#ov-throughput"), { series, unit: "ev/s", yMin: 0, xDomain: [now - 15 * 60000, now],
      label: "Validated and rejected events per second, last 15 minutes",
      emptyText: "Waiting for the first micro-batch" });

    const qBody = $("#ov-queries tbody");
    qBody.innerHTML = status.queries.map((q) => `<tr>
      <td>${LABELS.query[q.name]}</td><td>${stateTag(q.state)}</td>
      <td class="num nowrap">${q.last_batch_at ? ago(q.seconds_since_last_batch) : "–"}</td>
      <td class="num">${secs(q.last_batch_duration_ms)}</td></tr>`).join("");
    const p = status.producer;
    $("#ov-producer").textContent = !p.alive
      ? "Producer: no heartbeat. Start the producer container or check its logs."
      : p.simulation_running
        ? `Producer sending ${num(p.rate, 0)} events/s · heartbeat ${ago(p.seconds_since_heartbeat)}`
        : "Producer idle: simulation stopped. Start it from the Simulator view.";

    renderAlertRows("ov-alerts", alerts.items, true);
  }

  function renderAlertRows(tableId, items, withFinding) {
    if (!items.length) {
      emptyRow(tableId, withFinding ? 8 : 7, "No alerts yet. Each detector learns a per-device baseline first (about 5 minutes at the default rate), then alerts appear here.");
      return;
    }
    $(`#${tableId} tbody`).innerHTML = items.map((a) => `<tr tabindex="0" data-alert-id="${esc(a.alert_id)}" data-device="${esc(a.device_id)}">
      ${withFinding ? `<td><a class="ov-balloon" style="color:${SEV_COLOR[a.severity]}" href="#devices/${encodeURIComponent(a.device_id)}/${encodeURIComponent(a.alert_id)}" title="Alert ${a.device_seq} on ${esc(a.device_id)}: show it on the telemetry"><span>${a.device_seq ?? ""}</span></a></td>` : ""}
      <td>${sevMark(a.severity)}</td>
      <td class="nowrap">${when(a.detected_at)}</td>
      <td class="nowrap">${esc(a.device_id)}</td>
      <td>${LABELS.metric[a.metric_name] || esc(a.metric_name)}</td>
      <td>${LABELS.algorithm[a.algorithm] || esc(a.algorithm)}</td>
      ${withFinding ? `<td class="finding" title="${esc(a.description)}">${esc(a.description)}</td>` : `<td class="nowrap">${LABELS.kind[a.alert_kind] || esc(a.alert_kind)}</td>`}
      <td>${statusTag(a.status)}</td></tr>`).join("");
  }

  // --------------------------------------------------------------- pipeline
  async function loadPipeline() {
    const minutes = Number($("#pl-window").value);
    const bucket = minutes <= 15 ? 15 : minutes <= 60 ? 60 : 300;
    const [m, dl] = await Promise.all([
      api(`/pipeline/metrics?minutes=${minutes}&bucket_seconds=${bucket}`),
      api("/pipeline/dead-letters?limit=20"),
    ]);
    const now = Date.now();
    const xDomain = [now - minutes * 60000, now];
    const byQuery = (q) => m.series.filter((r) => r.query_name === q);
    const mk = (q, f) => ({ label: LABELS.query[q], color: QUERY_COLOR[q], points: byQuery(q).map((r) => [ts(r.bucket), f(r)]) });

    const rate = ["validator", "aggregator", "detector"].map((q) => mk(q, (r) => r.input_rows_per_sec));
    SPCharts.legend($("#pl-rate-legend"), rate);
    SPCharts.line($("#pl-rate"), { series: rate, yMin: 0, unit: "rows/s", xDomain, label: "Input rows per second by query" });

    const dur = ["validator", "aggregator", "detector"].map((q) => mk(q, (r) => (r.batch_duration_ms ?? NaN) / 1000));
    SPCharts.legend($("#pl-duration-legend"), dur);
    SPCharts.line($("#pl-duration"), { series: dur, yMin: 0, unit: "s", xDomain, label: "Micro-batch duration by query" });

    const lat = [
      { label: "Kafka → validated", color: "#3a3d9c", points: byQuery("validator").map((r) => [ts(r.bucket), (r.processing_latency_ms ?? NaN) / 1000]) },
      { label: "Event → validated", color: "#1d7470", dash: "4 3", points: byQuery("validator").map((r) => [ts(r.bucket), (r.end_to_end_latency_ms ?? NaN) / 1000]) },
      { label: "Event → detection", color: "#8a5a2b", points: byQuery("detector").map((r) => [ts(r.bucket), (r.end_to_end_latency_ms ?? NaN) / 1000]) },
    ];
    SPCharts.legend($("#pl-latency-legend"), lat);
    SPCharts.line($("#pl-latency"), { series: lat, yMin: 0, unit: "s", xDomain, label: "Latency" });

    bars($("#pl-reasons"), (m.rejections_by_reason || []).map((r) => ({ label: LABELS.reason[r.reason] || r.reason, value: Number(r.n), color: "#c0291f" })), 0);

    if (!dl.length) emptyRow("pl-deadletters", 4, "No rejected records. With data-quality faults enabled, malformed records appear here within seconds.");
    else $("#pl-deadletters tbody").innerHTML = dl.map((d) => `<tr>
      <td class="nowrap">${when(d.received_at)}</td>
      <td class="nowrap">${esc(LABELS.reason[d.reason] || d.reason)}</td>
      <td class="nowrap mono">p${esc(d.kafka_partition)} · ${esc(d.kafka_offset)}</td>
      <td class="finding mono" title="${esc(d.raw_payload)}">${esc(d.raw_payload)}</td></tr>`).join("");
  }

  // ---------------------------------------------------------------- devices
  const ACTIVE_MINUTES = 10;

  async function loadDevices() {
    const [active, all] = await Promise.all([api(`/devices?active_within_minutes=${ACTIVE_MINUTES}`), api("/devices")]);
    state.devices = active;
    const hidden = all.length - active.length;
    $("#dv-fleet-note").textContent = hidden > 0
      ? `${hidden} registered device${hidden === 1 ? "" : "s"} with no data in the last ${ACTIVE_MINUTES} minutes (earlier simulator runs or tests) not shown.`
      : "";
    const [selected, linkAlert] = (state.params || "").split("/").map(decodeURIComponent);
    if (linkAlert && state.linkFrom !== state.params) { state.linked = linkAlert; state.linkFrom = state.params; }
    const select = $("#dv-select");
    const options = state.devices.map((d) => `<option value="${esc(d.device_id)}">${esc(d.device_id)}</option>`).join("");
    if (select.dataset.options !== options) { select.innerHTML = '<option value="">Choose a device</option>' + options; select.dataset.options = options; }
    if (document.activeElement !== select) select.value = selected || "";
    const body = $("#dv-fleet tbody");
    if (!state.devices.length) {
      emptyRow("dv-fleet", 10, "No devices registered yet. They appear once the simulator starts publishing.");
    } else {
      body.innerHTML = state.devices.map((d) => `<tr tabindex="0" data-device="${esc(d.device_id)}" aria-selected="${d.device_id === selected}">
        <td class="nowrap"><b>${esc(d.device_id)}</b></td><td>${esc(d.plant_id)}</td><td>${esc((d.device_type || "").replace(/_/g, " "))}</td>
        <td class="num">${num(d.avg_temperature, 1)}</td><td class="num">${num(d.avg_vibration, 2)}</td>
        <td class="num">${num(d.avg_pressure, 1)}</td><td class="num">${num(d.avg_rpm, 0)}</td><td class="num">${num(d.avg_power_consumption, 2)}</td>
        <td>${d.open_alerts ? `${sevMark(d.worst_open_severity)} <span class="muted">${d.open_alerts}</span>` : '<span class="muted">none</span>'}</td>
        <td class="num nowrap">${d.last_window_end ? ago(d.last_window_end) : "–"}</td></tr>`).join("");
    }
    if (selected) await loadDeviceDetail(selected);
    else { $("#dv-detail").hidden = false; $("#dv-title").textContent = "No device selected"; $("#dv-charts").innerHTML = '<p class="muted">Choose a device to draw its telemetry plates.</p>'; $("#dv-alerts").innerHTML = ""; }
  }

  async function loadDeviceDetail(deviceId) {
    const minutes = Number($("#dv-range [aria-pressed='true']").dataset.min);
    const start = new Date(Date.now() - minutes * 60000).toISOString();
    const [hist, alerts] = await Promise.all([
      api(`/devices/${encodeURIComponent(deviceId)}/metrics?minutes=${minutes}`),
      api(`/alerts?device_id=${encodeURIComponent(deviceId)}&start=${encodeURIComponent(start)}&limit=200`),
    ]);
    const device = state.devices.find((d) => d.device_id === deviceId);
    $("#dv-detail").hidden = false;
    $("#dv-title").textContent = device ? `${deviceId} · ${device.plant_id} · ${(device.device_type || "").replace(/_/g, " ")}` : deviceId;

    const items = alerts.items.slice().sort((a, b) => ts(a.detected_at) - ts(b.detected_at));
    items.forEach((a) => { a.n = a.device_seq; });   // stable per-device item number, independent of range
    const w = hist.windows;
    const at = (r) => ts(r.window_end);
    const now = Date.now();
    const xDomain = [now - minutes * 60000, now];
    const charts = [
      { metric: "temperature", legend: [{ label: "1-min mean", color: "#3a3d9c" }, { label: "1-min min–max", color: "#3a3d9c", band: true }],
        series: [{ label: "1-min mean", color: "#3a3d9c", points: w.map((r) => [at(r), r.avg_temperature]),
          band: w.map((r) => [at(r), r.min_temperature, r.max_temperature]) }] },
      { metric: "vibration", legend: [{ label: "1-min mean", color: "#1d7470" }, { label: "1-min max", color: "#1d7470", dash: "3 3" }], series: [
          { label: "1-min mean", color: "#1d7470", points: w.map((r) => [at(r), r.avg_vibration]) },
          { label: "1-min max", color: "#1d7470", dash: "3 3", points: w.map((r) => [at(r), r.max_vibration]) }] },
      { metric: "pressure", legend: [{ label: "1-min mean", color: "#7a3b69" }, { label: "1-min min", color: "#7a3b69", dash: "3 3" }], series: [
          { label: "1-min mean", color: "#7a3b69", points: w.map((r) => [at(r), r.avg_pressure]) },
          { label: "1-min min", color: "#7a3b69", dash: "3 3", points: w.map((r) => [at(r), r.min_pressure]) }] },
      { metric: "rpm", legend: [{ label: "1-min mean", color: "#5f6a1f" }], series: [{ label: "1-min mean", color: "#5f6a1f", points: w.map((r) => [at(r), r.avg_rpm]) }] },
      { metric: "power_consumption", legend: [{ label: "1-min mean", color: "#8a5a2b" }], series: [{ label: "1-min mean", color: "#8a5a2b", points: w.map((r) => [at(r), r.avg_power_consumption]) }] },
    ];
    const host = $("#dv-charts");
    host.innerHTML = charts.map((c) => `<div class="chart" id="dv-chart-${c.metric}" data-height="${c.metric === "temperature" ? 190 : 150}"></div>`).join("");
    for (const c of charts) {
      const markers = items.filter((a) => a.metric_name === c.metric && a.observed_value !== null).map((a) => ({
        id: a.alert_id, n: a.n, t: ts(a.detected_at), v: a.observed_value, color: SEV_COLOR[a.severity],
        label: `${LABELS.algorithm[a.algorithm]}, ${a.severity}: ${a.description}`,
      }));
      SPCharts.line($(`#dv-chart-${c.metric}`), {
        legend: markers.length ? c.legend.concat([{ label: "alert reading", color: "#485158", dot: true }]) : c.legend,
        title: c.metric === "rpm" ? "Rotational speed · rpm" : `${LABELS.metric[c.metric]} · ${LABELS.unit[c.metric]}`, series: c.series, lane: true, markers, xDomain,
        unit: LABELS.unit[c.metric], label: `${LABELS.metric[c.metric]} for ${deviceId}`,
        onDraw: () => wireLinks(),
      });
    }

    const list = $("#dv-alerts");
    list.innerHTML = items.length
      ? items.slice().reverse().map((a) => `<li tabindex="0" data-alert-id="${esc(a.alert_id)}" style="color:${SEV_COLOR[a.severity]}">
          <span class="num-balloon">${a.n}</span>
          <div style="color:var(--graphite)">
            <div class="meta">${sevMark(a.severity)}<span>${when(a.detected_at)}</span><span>${LABELS.algorithm[a.algorithm]}</span><span>${LABELS.metric[a.metric_name]}</span>${statusTag(a.status)}</div>
            <p>${esc(a.description)}</p>
          </div></li>`).join("")
      : `<li class="empty">No alerts for this device in the last ${minutes < 60 ? minutes + " minutes" : minutes / 60 + " h"}.</li>`;
    wireLinks();
  }

  /** Cross-highlight a balloon on the chart and its entry in the callout list. */
  function applyLinked() {
    const host = $("#dv-detail");
    const id = state.linked;
    $$("[data-alert-id]", host).forEach((n) => n.classList.toggle("is-linked", Boolean(id) && n.dataset.alertId === id));
    $$(".chart", host).forEach((c) => c.classList.toggle("has-link", Boolean(id)));
  }

  function wireLinks() {
    const host = $("#dv-detail");
    // The highlight lives in state so the 5 s polling redraw re-applies it instead of dropping it.
    const setLinked = (id) => { state.linked = id; applyLinked(); };
    applyLinked();
    $$("[data-alert-id]", host).forEach((n) => {
      if (n.dataset.wired) return;
      n.dataset.wired = "1";
      n.addEventListener("mouseenter", () => setLinked(n.dataset.alertId));
      n.addEventListener("focus", () => setLinked(n.dataset.alertId));
      n.addEventListener("mouseleave", () => setLinked(null));
      n.addEventListener("blur", () => setLinked(null));
      const open = () => { location.hash = `#alerts/${encodeURIComponent(n.dataset.alertId)}`; };
      n.addEventListener("click", open);
      n.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); } });
    });
  }

  // ----------------------------------------------------------------- alerts
  function alertQuery() {
    const f = $("#al-filters");
    const params = new URLSearchParams();
    const sev = $$("input[name=severity]:checked", f).map((i) => i.value);
    if (sev.length && sev.length < 4) sev.forEach((s) => params.append("severity", s));
    for (const name of ["algorithm", "status", "device_id"]) {
      const v = f.elements[name].value;
      if (v) params.set(name, v);
    }
    const hours = Number(f.elements.hours.value);
    params.set("start", new Date(Date.now() - hours * 3600000).toISOString());
    params.set("limit", state.alertPage);
    params.set("offset", state.alertOffset);
    return { params, hours, sevCount: sev.length };
  }

  async function loadAlerts() {
    const { params, hours, sevCount } = alertQuery();
    const deviceSelect = $("#al-filters select[name=device_id]");
    if (deviceSelect.options.length <= 1) {
      if (!state.devices.length) state.devices = await api("/devices");
      deviceSelect.insertAdjacentHTML("beforeend", state.devices.map((d) => `<option value="${esc(d.device_id)}">${esc(d.device_id)}</option>`).join(""));
    }
    const [page, summary, evaluation] = await Promise.all([
      sevCount === 0 ? Promise.resolve({ total: 0, items: [] }) : api(`/alerts?${params}`),
      api(`/alerts/summary?hours=${hours}`),
      api(`/alerts/evaluation?hours=${hours}`),
    ]);
    const from = page.total ? state.alertOffset + 1 : 0;
    const to = state.alertOffset + page.items.length;
    $("#al-count").textContent = `Alerts · ${num(from)}–${num(to)} of ${num(page.total)}`;
    $("#al-prev").disabled = state.alertOffset === 0;
    $("#al-next").disabled = to >= page.total;
    if (!page.items.length) {
      emptyRow("al-table", 7, sevCount === 0 ? "Select at least one severity." : "No alerts match these filters. Widen the range or clear a filter.");
    } else {
      renderAlertRows("al-table", page.items, false);
    }
    const selected = decodeURIComponent(state.params || "");
    $$("#al-table tbody tr").forEach((tr) => tr.setAttribute("aria-selected", String(tr.dataset.alertId === selected)));

    $("#al-dist-range").textContent = `Last ${hours < 24 ? hours + " h" : hours / 24 + " d"} · ${num(summary.total)} alerts`;
    const dist = $("#al-distribution");
    dist.innerHTML = `<div><h3>Severity</h3><div id="d-sev" class="bars"></div></div><div><h3>Algorithm</h3><div id="d-alg" class="bars"></div></div><div><h3>Kind</h3><div id="d-kind" class="bars"></div></div>`;
    bars($("#d-sev"), ["critical", "high", "medium", "low"].map((s) => ({ label: s[0].toUpperCase() + s.slice(1), value: summary.by_severity[s] || 0, color: SEV_COLOR[s] })), summary.total);
    bars($("#d-alg"), ["threshold", "zscore", "cusum", "drift"].map((a) => ({ label: LABELS.algorithm[a], value: summary.by_algorithm[a] || 0 })), summary.total);
    bars($("#d-kind"), Object.keys(LABELS.kind).map((k) => ({ label: LABELS.kind[k], value: summary.by_kind[k] || 0 })), summary.total);

    const rec = {};
    for (const r of evaluation.recall) {
      const e = (rec[r.algorithm] ||= { episodes: 0, detected: 0, delaySum: 0 });
      e.episodes += Number(r.episodes); e.detected += Number(r.detected);
      if (r.mean_delay_s !== null) e.delaySum += Number(r.mean_delay_s) * Number(r.detected);
    }
    const prec = Object.fromEntries(evaluation.precision.map((r) => [r.algorithm, r]));
    $("#al-eval tbody").innerHTML = ["threshold", "zscore", "cusum", "drift"].map((a) => {
      const p = prec[a], r = rec[a];
      return `<tr><td>${LABELS.algorithm[a]}</td><td class="num">${num(p ? p.alerts : 0)}</td>
        <td class="num">${p && p.alerts ? pct(p.matched / p.alerts) : "–"}</td>
        <td class="num">${r && r.episodes ? pct(r.detected / r.episodes) : "–"}</td>
        <td class="num">${r && r.detected ? secs((1000 * r.delaySum) / r.detected) : "–"}</td></tr>`;
    }).join("");

    if (selected) await loadAlertDetail(selected, false);
  }

  async function loadAlertDetail(alertId, force) {
    const panel = $("#al-detail");
    if (!force && panel.dataset.alertId === alertId && panel.contains(document.activeElement)) return;
    let a;
    try { a = await api(`/alerts/${encodeURIComponent(alertId)}`); } catch (e) {
      panel.innerHTML = `<p class="empty">This alert no longer exists.</p>`;
      return;
    }
    const note = panel.dataset.alertId === alertId ? ($("#al-note") || {}).value || "" : "";
    panel.dataset.alertId = alertId;
    const resolved = a.status === "resolved";
    const verdict = a.confirmed_failure === true ? "Confirmed failure" : a.confirmed_failure === false ? "False alarm" : "Resolved, cause unknown";
    panel.innerHTML = `
      <div class="meta">${sevMark(a.severity)} ${statusTag(a.status)}</div>
      <h3>${LABELS.kind[a.alert_kind]} in ${LABELS.metric[a.metric_name].toLowerCase()} on ${esc(a.device_id)}</h3>
      <p class="desc">${esc(a.description)}</p>
      <div class="readings">
        <div><span>Observed</span><b>${fmtValue(a.observed_value, a.metric_name)}</b></div>
        <div><span>${a.algorithm === "threshold" ? "Limit" : "Baseline"}</span><b>${fmtValue(a.baseline_value, a.metric_name)}</b></div>
        <div><span>Score</span><b>${num(a.deviation_score, 2)}</b></div>
      </div>
      <dl class="facts">
        <dt>Algorithm</dt><dd>${LABELS.algorithm[a.algorithm]}</dd>
        <dt>Detected (event time)</dt><dd>${when(a.detected_at)}</dd>
        <dt>Persisted</dt><dd>${when(a.persisted_at)} <span class="muted">(+${secs(ts(a.persisted_at) - ts(a.detected_at))})</span></dd>
        <dt>Plant</dt><dd>${esc(a.plant_id || "–")}</dd>
        <dt>Triggering event</dt><dd class="mono" title="${esc(a.event_id)}">${esc(a.event_id).replace(/-/g, "-<wbr>")}</dd>
        ${a.acknowledged_at ? `<dt>Acknowledged</dt><dd>${when(a.acknowledged_at)} · ${esc(a.acknowledged_by || "")}</dd>` : ""}
        ${resolved ? `<dt>Resolution</dt><dd>${verdict}</dd>` : ""}
        ${a.resolution_note ? `<dt>Note</dt><dd>${esc(a.resolution_note)}</dd>` : ""}
      </dl>
      <p class="evidence-note">This is statistical evidence of change. Record whether it matched a real failure when you resolve it.</p>
      ${resolved ? "" : `
      <label class="field">Resolution note<textarea id="al-note" maxlength="2000" placeholder="What did inspection find?">${esc(note)}</textarea></label>
      <div class="actions">
        ${a.status === "open" ? '<button type="button" class="btn" data-act="ack">Acknowledge</button>' : ""}
        <button type="button" class="btn btn-danger" data-act="confirm">Resolve: confirmed failure</button>
        <button type="button" class="btn" data-act="false">Resolve: false alarm</button>
        <span class="form-msg" id="al-msg" role="status"></span>
      </div>`}
      <a class="link" href="#devices/${encodeURIComponent(a.device_id)}">Show ${esc(a.device_id)} telemetry</a>`;
    $$("[data-act]", panel).forEach((b) => b.addEventListener("click", () => act(a.alert_id, b.dataset.act, b)));
  }

  async function act(alertId, action, button) {
    const msg = $("#al-msg");
    button.setAttribute("aria-busy", "true");
    $$("[data-act]").forEach((b) => (b.disabled = true));
    try {
      if (action === "ack") {
        await api(`/alerts/${encodeURIComponent(alertId)}/acknowledge`, { method: "POST", body: JSON.stringify({ acknowledged_by: "dashboard" }) });
      } else {
        await api(`/alerts/${encodeURIComponent(alertId)}/resolve`, {
          method: "POST",
          body: JSON.stringify({ confirmed_failure: action === "confirm", note: ($("#al-note") || {}).value || null }),
        });
      }
      await loadAlertDetail(alertId, true);
      await loadAlerts();
    } catch (e) {
      if (msg) { msg.textContent = `Could not save: ${e.message}. Try again.`; msg.classList.add("error"); }
      $$("[data-act]").forEach((b) => (b.disabled = false));
      button.removeAttribute("aria-busy");
    }
  }

  // -------------------------------------------------------------- simulator
  const SIM_FIELDS = ["devices", "rate", "anomaly_rate", "intensity", "seed"];
  let simState = null;

  async function loadSimulator() {
    simState = await api("/simulation/status");
    const s = simState;
    const form = $("#sim-form");
    if (!state.simDirty) {
      for (const f of SIM_FIELDS) form.elements[f].value = s[f];
      form.elements.data_quality.checked = s.data_quality;
    }
    $("#sim-start").textContent = s.running ? "Apply changes" : "Start";
    $("#sim-stop").disabled = !s.running;
    $("#sim-state").innerHTML = `
      <dt>Simulation</dt><dd>${s.running ? stateTag("running") : stateTag("stalled", "stopped")}</dd>
      <dt>Producer process</dt><dd>${s.producer_alive ? stateTag("ok", "alive") : stateTag("down", "no heartbeat")}</dd>
      <dt>Last heartbeat</dt><dd>${s.producer_heartbeat_at ? ago(s.producer_heartbeat_at) : "never"}</dd>
      <dt>Current send rate</dt><dd>${num(s.producer_rate, 0)} events/s</dd>
      <dt>Events sent (this process)</dt><dd>${num(s.events_sent)}</dd>
      <dt>Interval per device</dt><dd>${num(s.devices / s.rate, 2)} s</dd>
      <dt>Configuration version</dt><dd>${num(s.config_version)}</dd>
      <dt>Last change</dt><dd>${ago(s.updated_at)}</dd>`;
  }

  function wireSimulator() {
    const form = $("#sim-form");
    const msg = $("#sim-msg");
    form.addEventListener("input", () => { state.simDirty = true; });
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      if (!form.reportValidity()) return;
      const body = {};
      for (const f of SIM_FIELDS) {
        const v = Number(form.elements[f].value);
        if (!simState || Number(simState[f]) !== v) body[f] = v;
      }
      if (!simState || simState.data_quality !== form.elements.data_quality.checked) body.data_quality = form.elements.data_quality.checked;
      const btn = $("#sim-start");
      btn.setAttribute("aria-busy", "true"); btn.disabled = true;
      msg.classList.remove("error");
      try {
        await api("/simulation/start", { method: "POST", body: JSON.stringify(body) });
        state.simDirty = false;
        msg.textContent = Object.keys(body).length ? "Applied. The producer restarts the fleet within a second." : "Started.";
        await loadSimulator();
      } catch (err) {
        msg.textContent = `Could not start: ${err.message}.`; msg.classList.add("error");
      } finally { btn.removeAttribute("aria-busy"); btn.disabled = false; }
    });
    $("#sim-stop").addEventListener("click", async () => {
      msg.classList.remove("error");
      try { await api("/simulation/stop", { method: "POST" }); msg.textContent = "Stopped. Spark keeps running and drains what is in Kafka."; await loadSimulator(); }
      catch (err) { msg.textContent = `Could not stop: ${err.message}.`; msg.classList.add("error"); }
    });
  }

  // ----------------------------------------------------------------- router
  const LOADERS = { overview: loadOverview, pipeline: loadPipeline, devices: loadDevices, alerts: loadAlerts, simulator: loadSimulator };

  function parseHash() {
    const raw = (location.hash || "#overview").slice(1);
    const [view, ...rest] = raw.split("/");
    return { view: LOADERS[view] ? view : "overview", params: rest.join("/") };
  }

  async function refresh() {
    if (state.inflight || document.hidden) return;
    state.inflight = true;
    try {
      const ok = await refreshStatus();
      if (ok !== null) await LOADERS[state.view]();
    } catch (e) {
      console.warn("refresh failed", e);
    } finally {
      state.inflight = false;
    }
  }

  function route() {
    const { view, params } = parseHash();
    const changedView = view !== state.view;
    state.view = view;
    state.params = params;
    $$(".view").forEach((v) => (v.hidden = v.dataset.view !== view));
    $$(".nav a").forEach((a) => { if (a.dataset.view === view) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
    document.title = `${view[0].toUpperCase() + view.slice(1)} · StreamPulse Engine`;
    if (changedView) { window.scrollTo(0, 0); $("#main").focus({ preventScroll: true }); }
    state.inflight = false;
    refresh();
  }

  function wire() {
    window.addEventListener("hashchange", route);
    document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });

    const openRow = (tr) => {
      if (tr.dataset.alertId) location.hash = `#alerts/${encodeURIComponent(tr.dataset.alertId)}`;
      else if (tr.dataset.device) location.hash = `#devices/${encodeURIComponent(tr.dataset.device)}`;
    };
    for (const id of ["ov-alerts", "al-table", "dv-fleet"]) {
      const tbody = $(`#${id} tbody`);
      tbody.addEventListener("click", (e) => { if (e.target.closest("a")) return; const tr = e.target.closest("tr[tabindex]"); if (tr) openRow(tr); });
      tbody.addEventListener("keydown", (e) => {
        const tr = e.target.closest("tr[tabindex]");
        if (tr && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); openRow(tr); }
      });
    }
    $("#pl-window").addEventListener("change", refresh);
    $("#dv-select").addEventListener("change", (e) => { location.hash = e.target.value ? `#devices/${encodeURIComponent(e.target.value)}` : "#devices"; });
    $("#dv-range").addEventListener("click", (e) => {
      const b = e.target.closest("button");
      if (!b) return;
      $$("#dv-range button").forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      refresh();
    });
    $("#al-filters").addEventListener("change", () => { state.alertOffset = 0; refresh(); });
    $("#al-prev").addEventListener("click", () => { state.alertOffset = Math.max(0, state.alertOffset - state.alertPage); refresh(); });
    $("#al-next").addEventListener("click", () => { state.alertOffset += state.alertPage; refresh(); });
    wireSimulator();
  }

  wire();
  route();
  state.timer = setInterval(refresh, POLL_MS);
})();
