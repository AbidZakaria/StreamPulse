/* Minimal SVG time-series charts with drafting-style balloon callouts.
   No dependencies. Each chart redraws itself when its container resizes. */
(function () {
  "use strict";
  const NS = "http://www.w3.org/2000/svg";
  const MIN = 60_000;
  const TIME_STEPS = [MIN, 2 * MIN, 5 * MIN, 10 * MIN, 15 * MIN, 30 * MIN, 60 * MIN, 120 * MIN, 180 * MIN, 360 * MIN];

  function el(name, attrs, parent) {
    const node = document.createElementNS(NS, name);
    for (const k in attrs) if (attrs[k] !== undefined && attrs[k] !== null) node.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(node);
    return node;
  }

  function niceStep(span, count) {
    const raw = span / Math.max(count, 1);
    const pow = Math.pow(10, Math.floor(Math.log10(raw)));
    const n = raw / pow;
    return (n < 1.5 ? 1 : n < 3 ? 2 : n < 7 ? 5 : 10) * pow;
  }

  function niceDomain(min, max, count) {
    if (!isFinite(min) || !isFinite(max)) return { min: 0, max: 1, step: 0.25 };
    if (min === max) { const d = Math.abs(min) * 0.1 || 1; min -= d; max += d; }
    const step = niceStep(max - min, count);
    return { min: Math.floor(min / step) * step, max: Math.ceil(max / step) * step, step };
  }

  function fmtTime(ms, withSeconds) {
    const d = new Date(ms);
    const p = (n) => String(n).padStart(2, "0");
    return `${p(d.getHours())}:${p(d.getMinutes())}${withSeconds ? ":" + p(d.getSeconds()) : ""}`;
  }

  function defaultFormat(v) {
    const a = Math.abs(v);
    if (a >= 10000) return (v / 1000).toFixed(0) + "k";
    if (a >= 1000) return (v / 1000).toFixed(1) + "k";
    if (a >= 100) return v.toFixed(0);
    if (a >= 10) return v.toFixed(1);
    return v.toFixed(2);
  }

  /** Split a series where consecutive points are further apart than `gap` ms. */
  function segments(points, gap) {
    const out = [];
    let cur = [];
    for (let i = 0; i < points.length; i++) {
      if (cur.length && points[i][0] - cur[cur.length - 1][0] > gap) { out.push(cur); cur = []; }
      cur.push(points[i]);
    }
    if (cur.length) out.push(cur);
    return out;
  }

  function medianSpacing(points) {
    if (points.length < 3) return Infinity;
    const d = [];
    for (let i = 1; i < points.length; i++) d.push(points[i][0] - points[i - 1][0]);
    d.sort((a, b) => a - b);
    return d[Math.floor(d.length / 2)];
  }

  function draw(container, cfg) {
    const width = Math.max(container.clientWidth, 240);
    const height = cfg.height || Number(container.dataset.height) || 200;
    const hasMarkers = cfg.markers && cfg.markers.length > 0;
    // `lane: true` reserves the balloon lane even without markers, so stacked plates share one header band.
    const m = { top: (cfg.title ? 22 : 8) + (hasMarkers || cfg.lane ? 30 : 0), right: 14, bottom: 24, left: 48 };
    const fmtY = cfg.formatY || defaultFormat;

    container.textContent = "";
    const svg = el("svg", { viewBox: `0 0 ${width} ${height}`, height, role: "img", "aria-label": cfg.label || "" }, container);

    const all = [];
    for (const s of cfg.series) {
      for (const p of s.points) if (p[1] !== null && isFinite(p[1])) all.push(p);
      for (const b of s.band || []) { if (isFinite(b[1])) all.push([b[0], b[1]]); if (isFinite(b[2])) all.push([b[0], b[2]]); }
    }
    for (const mk of cfg.markers || []) if (isFinite(mk.v)) all.push([mk.t, mk.v]);

    let titleEnd = 0;
    if (cfg.title) {
      const tt = el("text", { x: 0, y: 12, class: "chart-title" }, svg);
      tt.textContent = cfg.title;
      titleEnd = tt.getComputedTextLength ? tt.getComputedTextLength() : cfg.title.length * 7;
    }
    if (cfg.legend && cfg.legend.length) {
      // series key, right-aligned in the title row
      const lg = el("g", { class: "plate-legend" }, svg);
      let x = width - m.right;
      // drawn right to left in order; items that would collide with the title are left out
      for (const item of cfg.legend) {
        const t = el("text", { x, y: 12, "text-anchor": "end" }, lg);
        t.textContent = item.label;
        const w = t.getComputedTextLength ? t.getComputedTextLength() : item.label.length * 6;
        if (x - w - 22 < titleEnd + 16) { t.remove(); continue; }
        x -= w + 6;
        if (item.dot) el("circle", { cx: x - 8, cy: 8.5, r: 3, fill: item.color }, lg);
        else if (item.band) el("rect", { x: x - 16, y: 4, width: 16, height: 9, fill: item.color, opacity: 0.2 }, lg);
        else el("line", { x1: x - 16, x2: x, y1: 8.5, y2: 8.5, stroke: item.color, "stroke-width": 1.75,
                          "stroke-dasharray": item.dash || null }, lg);
        x -= 16 + 14;
      }
    }

    const plotW = width - m.left - m.right;
    const plotH = height - m.top - m.bottom;
    if (!all.length) {
      el("rect", { x: m.left, y: m.top, width: plotW, height: plotH, fill: "none", stroke: "var(--rule)" }, svg);
      el("text", { x: m.left + plotW / 2, y: m.top + plotH / 2, "text-anchor": "middle", class: "empty-chart" }, svg)
        .textContent = cfg.emptyText || "No data in this range yet";
      return null;
    }

    const xs = all.map((p) => p[0]);
    const x0 = cfg.xDomain ? cfg.xDomain[0] : Math.min(...xs);
    const x1 = cfg.xDomain ? cfg.xDomain[1] : Math.max(...xs);
    let yMin = Math.min(...all.map((p) => p[1]));
    let yMax = Math.max(...all.map((p) => p[1]));
    if (cfg.yMin !== undefined) yMin = Math.min(yMin, cfg.yMin);
    const pad = (yMax - yMin) * 0.08;
    const dom = niceDomain(yMin - (cfg.yMin === 0 ? 0 : pad), yMax + pad, Math.max(2, Math.min(5, Math.floor(plotH / 34))));
    // Tick labels carry exactly as many decimals as the tick step needs (1400, 1425 not 1.4k, 1.4k).
    const decimals = Math.max(0, Math.min(3, -Math.floor(Math.log10(dom.step) + 1e-9)));
    const fmtTick = cfg.formatTick || ((v) => v.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals }));
    const X = (t) => m.left + ((t - x0) / Math.max(x1 - x0, 1)) * plotW;
    const Y = (v) => m.top + plotH - ((v - dom.min) / (dom.max - dom.min)) * plotH;

    // grid + y axis
    const grid = el("g", { class: "grid" }, svg);
    const axis = el("g", { class: "axis" }, svg);
    for (let v = dom.min; v <= dom.max + dom.step / 2; v += dom.step) {
      const y = Math.round(Y(v)) + 0.5;
      el("line", { x1: m.left, x2: m.left + plotW, y1: y, y2: y }, grid);
      el("text", { x: m.left - 8, y: y + 4, "text-anchor": "end" }, axis).textContent = fmtTick(v);
    }
    // x axis
    const span = x1 - x0;
    const step = TIME_STEPS.find((s) => span / s <= Math.max(3, Math.floor(plotW / 110))) || TIME_STEPS[TIME_STEPS.length - 1];
    const first = Math.ceil(x0 / step) * step;
    for (let t = first; t <= x1; t += step) {
      const x = Math.round(X(t)) + 0.5;
      if (x < m.left + 20 || x > m.left + plotW - 16) continue;  // keep clear of the y labels and the edge
      el("line", { x1: x, x2: x, y1: m.top + plotH, y2: m.top + plotH + 4, stroke: "var(--rule-strong)" }, axis);
      el("text", { x, y: m.top + plotH + 17, "text-anchor": "middle" }, axis).textContent = fmtTime(t, step < MIN);
    }
    el("line", { class: "baseline", x1: m.left, x2: m.left + plotW, y1: m.top + plotH + 0.5, y2: m.top + plotH + 0.5 }, svg);

    // bands + series, clipped to the plot area (sliding windows may end just after "now")
    const clipId = `clip-${Math.random().toString(36).slice(2, 9)}`;
    el("rect", { x: m.left, y: m.top - 2, width: plotW, height: plotH + 4 }, el("clipPath", { id: clipId }, el("defs", {}, svg)));
    const plot = el("g", { "clip-path": `url(#${clipId})` }, svg);
    for (const s of cfg.series) {
      if (s.band && s.band.length > 1) {
        const gap = medianSpacing(s.band) * 3;
        for (const seg of segments(s.band.filter((b) => isFinite(b[1]) && isFinite(b[2])), gap)) {
          if (seg.length < 2) continue;
          const top = seg.map((b) => `${X(b[0]).toFixed(1)},${Y(b[2]).toFixed(1)}`);
          const bot = seg.slice().reverse().map((b) => `${X(b[0]).toFixed(1)},${Y(b[1]).toFixed(1)}`);
          el("polygon", { points: top.concat(bot).join(" "), fill: s.color, class: "band" }, plot);
        }
      }
      const pts = s.points.filter((p) => p[1] !== null && isFinite(p[1]));
      const gap = medianSpacing(pts) * 3;
      for (const seg of segments(pts, gap)) {
        if (seg.length === 1) { el("circle", { cx: X(seg[0][0]), cy: Y(seg[0][1]), r: 2, fill: s.color }, plot); continue; }
        const d = seg.map((p, i) => `${i ? "L" : "M"}${X(p[0]).toFixed(1)},${Y(p[1]).toFixed(1)}`).join("");
        el("path", { d, class: "series", stroke: s.color, "stroke-dasharray": s.dash || null }, plot);
      }
    }

    // balloons: anchor dot at the onset, leader to a numbered circle in the lane above
    let balloons = [];
    if (hasMarkers) {
      const laneY = m.top - 18;
      const r = 9;
      const sorted = cfg.markers.slice().sort((a, b) => a.t - b.t).map((mk) => ({ ...mk, ax: X(mk.t), ay: Y(mk.v) }));
      let prev = -Infinity;
      for (const mk of sorted) { mk.bx = Math.max(mk.ax, prev + 2 * r + 4, m.left + r); prev = mk.bx; }
      const maxX = m.left + plotW - r;
      for (let i = sorted.length - 1, next = Infinity; i >= 0; i--) {
        sorted[i].bx = Math.min(sorted[i].bx, maxX, next - 2 * r - 4);
        next = sorted[i].bx;
      }
      for (const mk of sorted) {
        const g = el("g", { class: "balloon", "data-alert-id": mk.id, style: `color:${mk.color}`, tabindex: 0,
                            role: "button", "aria-label": `Alert ${mk.n}: ${mk.label || ""}` }, svg);
        const dx = mk.bx - mk.ax, dy = laneY - mk.ay, len = Math.hypot(dx, dy) || 1;
        el("line", { class: "leader", x1: mk.ax, y1: mk.ay, x2: mk.bx - (dx / len) * r, y2: laneY - (dy / len) * r, stroke: mk.color }, g);
        el("circle", { class: "onset", cx: mk.ax, cy: mk.ay, r: 3.2, fill: mk.color }, g);   // the alert's reading
        el("circle", { cx: mk.bx, cy: laneY, r, stroke: mk.color }, g);
        el("text", { x: mk.bx, y: laneY + 0.5, fill: mk.color }, g).textContent = mk.n;
        el("title", {}, g).textContent = mk.label || "";
        balloons.push(g);
      }
    }

    // hover readout
    const hoverLine = el("line", { class: "hover-line", y1: m.top, y2: m.top + plotH, visibility: "hidden" }, svg);
    const overlay = el("rect", { x: m.left, y: m.top, width: plotW, height: plotH, fill: "transparent" }, svg);
    let tip = null;
    const nearest = (pts, t) => {
      let best = null, bd = Infinity;
      for (const p of pts) { const d = Math.abs(p[0] - t); if (d < bd) { bd = d; best = p; } }
      return best;
    };
    overlay.addEventListener("mousemove", (ev) => {
      const rect = svg.getBoundingClientRect();
      const px = (ev.clientX - rect.left) * (width / rect.width);
      const t = x0 + ((px - m.left) / plotW) * (x1 - x0);
      const rows = cfg.series.map((s) => [s, nearest(s.points.filter((p) => p[1] !== null), t)]).filter((r) => r[1]);
      if (!rows.length) return;
      const at = rows[0][1][0];
      hoverLine.setAttribute("x1", X(at)); hoverLine.setAttribute("x2", X(at)); hoverLine.setAttribute("visibility", "visible");
      if (!tip) { tip = document.createElement("div"); tip.className = "chart-tip"; container.appendChild(tip); }
      tip.innerHTML = `<b>${fmtTime(at, true)}</b><br>` + rows.map(([s, p]) =>
        `<i class="sw" style="background:${s.tipColor || s.color}"></i>${s.label}: ${fmtY(p[1])}${cfg.unit ? " " + cfg.unit : ""}`).join("<br>");
      const left = (X(at) / width) * rect.width;
      tip.style.left = `${Math.min(left + 12, rect.width - tip.offsetWidth - 4)}px`;
      tip.style.top = `${m.top}px`;
    });
    overlay.addEventListener("mouseleave", () => {
      hoverLine.setAttribute("visibility", "hidden");
      if (tip) { tip.remove(); tip = null; }
    });
    return { svg, balloons };
  }

  const observed = new WeakMap();
  const ro = typeof ResizeObserver !== "undefined"
    ? new ResizeObserver((entries) => {
        for (const e of entries) {
          const state = observed.get(e.target);
          if (state && Math.abs(e.contentRect.width - state.width) > 2) {
            state.width = e.contentRect.width;
            state.result = draw(e.target, state.cfg);
            if (state.cfg.onDraw) state.cfg.onDraw(state.result);
          }
        }
      })
    : null;

  function line(container, cfg) {
    const result = draw(container, cfg);
    if (cfg.onDraw) cfg.onDraw(result);
    if (ro) {
      if (!observed.has(container)) ro.observe(container);
      observed.set(container, { cfg, width: container.clientWidth, result });
    }
    return result;
  }

  function legend(target, series) {
    target.innerHTML = series.map((s) =>
      `<span><i style="background:${s.color}${s.dash ? ";background:repeating-linear-gradient(90deg," + s.color + " 0 4px,transparent 4px 7px)" : ""}"></i>${s.label}</span>`).join("");
  }

  window.SPCharts = { line, legend, fmtTime };
})();
