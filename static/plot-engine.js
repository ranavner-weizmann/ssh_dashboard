// Shared time-series overlay plotting engine - one or more series drawn
// against a real (not index-based) time x-axis, with scroll-to-zoom,
// drag-to-pan, and double-click-to-reset. Used by both the online Data
// Viewer (templates/index.html, reading over SFTP) and the offline
// merged-data viewer (templates/offline.html, reading local files) -
// the drawing/interaction code is identical either way, only where the
// points come from differs, so it lives here once rather than twice.

const PLOT_PADDING = { top: 16, right: 16, bottom: 24, left: 46 };


function plotTimeOnly(ts) {
  // "2026-07-06 15:15:11" -> "15:15:11"
  const parts = String(ts).split(' ');
  return parts.length > 1 ? parts[1] : ts;
}

function parseTsMs(ts) {
  // "2026-07-06 15:15:11" -> epoch ms. Only ever compared against other
  // parsed timestamps from the same file, never against wall-clock time,
  // so the local-time interpretation JS gives a zoneless string is fine.
  const ms = Date.parse(String(ts).replace(' ', 'T'));
  return Number.isNaN(ms) ? null : ms;
}

// Inverse of parseTsMs, for axis labels at an arbitrary zoomed-in
// boundary that doesn't necessarily land exactly on a real data point.
function formatMsAsTime(ms) {
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function getCssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function hexToRgba(hex, alpha) {
  const h = hex.replace('#', '');
  const r = parseInt(h.substring(0, 2), 16);
  const g = parseInt(h.substring(2, 4), 16);
  const b = parseInt(h.substring(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

// "pom_Ozone_ppb" -> "pom" - every per-run/merged-day CSV in this
// project prefixes a column with the instrument it came from, up to
// the first underscore (see _merge_day's docstring in app.py). Shared
// by both the online Data Viewer and the offline merged-data viewer's
// instrument/variable two-level column pickers, so a file with many
// instruments' worth of columns (merged_data.csv) doesn't have to be
// one long alphabetical list.
function columnInstrumentOf(column) {
  const idx = column.indexOf('_');
  return idx === -1 ? column : column.slice(0, idx);
}

// "pom_Ozone_ppb" -> "Ozone_ppb" - the same column, minus its
// instrument prefix, for display once that instrument's already
// chosen (repeating "pom_" on every option in its own list would be
// redundant).
function columnFieldLabelOf(column) {
  const idx = column.indexOf('_');
  return idx === -1 ? column : column.slice(idx + 1);
}

// True only when at least two distinct instrument prefixes each cover
// 2+ of the given columns - the real "this file mixes several
// instruments" signal (merged_data.csv, where "imet"/"pom"/"trisonica"/
// ... each own a dozen-odd columns). A raw per-sensor CSV's own column
// names (pressure, temp, rel_hum, hum_temp, ...) or a flat file like
// drone telemetry (latitude_deg, altitude_m, pitch_deg, ...) has no
// instrument prefix at all - just ordinary snake_case field names - so a
// naive "2+ distinct first-underscore-tokens" check would trigger on
// almost any multi-column CSV. Requiring each prefix to actually recur
// is what tells a real instrument grouping apart from an incidental
// underscore in an otherwise unrelated field name.
function hasMultipleInstruments(columns) {
  const counts = new Map();
  columns.forEach((name) => {
    const inst = columnInstrumentOf(name);
    counts.set(inst, (counts.get(inst) || 0) + 1);
  });
  let repeated = 0;
  counts.forEach((c) => { if (c >= 2) repeated++; });
  return repeated >= 2;
}

// True only for a merged_data.csv file - the one file shape in this
// project whose columns are genuinely prefixed by instrument at the top
// level (see _merge_day's docstring in app.py: "imet_*, pom_*,
// trisonica_*, ..."). hasMultipleInstruments' "2+ prefixes each with 2+
// members" signal isn't reliable on its own - some raw per-sensor CSVs
// have that same shape purely by coincidence within a single instrument
// (the cavity laser controller's own fields group under LDD_/TEC_/
// pressure_, e.g. pressure_mb + pressure_status - not different
// instruments, just that one sensor's own naming). Requiring the
// filename shape unique to a real instrument merge is what tells the
// two apart.
function isMergedDataFile(relpath) {
  const base = relpath.split('/').pop();
  return base.startsWith('merged_data_');
}


// Shared by both the live plots (createPlotController, defined inline in
// index.html) and this file's createStaticPlotController - the canvas
// resize mechanics are identical either way, only where the points come
// from differs.

function getCanvasContext(canvas) {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const targetW = Math.max(1, Math.round(rect.width * dpr));
  const targetH = Math.max(1, Math.round(rect.height * dpr));
  if (canvas.width !== targetW || canvas.height !== targetH) {
    canvas.width = targetW;
    canvas.height = targetH;
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, width: rect.width, height: rect.height };
}


// Nearest point to targetMs in a points array already sorted ascending by
// .ms (binary search - these arrays can run into the thousands of rows).
function nearestPointByTime(points, targetMs) {
  if (!points.length) return null;
  let lo = 0, hi = points.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (points[mid].ms < targetMs) lo = mid + 1; else hi = mid;
  }
  if (lo > 0 && Math.abs(points[lo - 1].ms - targetMs) <= Math.abs(points[lo].ms - targetMs)) {
    return points[lo - 1];
  }
  return points[lo];
}

// Draws one or more time series sharing a real (not index-based) time
// x-axis, so overlaid variables line up by actual timestamp even when
// they don't share the same set of valid rows (e.g. merged_data's
// per-sensor columns, each populated on a different subset of rows).
//
// A single series is drawn exactly like the old single-column plot
// (real value gridlines, area wash, accent color). Two or more series
// are never given a second y-scale - see the dataviz skill's "no
// dual-axis" rule - each is instead min-max scaled to its own range on
// one shared 0-100% axis, so differently-scaled variables (temperature
// vs. particle counts) can share a plot without inventing a spurious
// alignment. Actual values live in the tooltip and the legend chips.
//
// viewWindow ({min, max} in ms, or null for the full data extent) lets
// the caller zoom - the y-axis re-scales to whatever's actually visible
// in that window, same as panning/zooming in any time-series chart,
// rather than staying fixed to the full-data range.
//
// Returns {tMin, tMax, fullTMin, fullTMax} - the window actually drawn
// plus the underlying data's full extent - so the caller's hover
// handler and zoom/pan math don't have to recompute either.
function drawOverlayPlot(canvas, series, hoverMs, emptyMessage, viewWindow) {
  const { ctx, width, height } = getCanvasContext(canvas);
  ctx.clearRect(0, 0, width, height);

  const withPoints = series.filter((s) => s.points.length > 0);
  if (!withPoints.length) {
    ctx.fillStyle = getCssVar('--text-dim');
    ctx.font = '13px ' + getCssVar('--sans');
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(emptyMessage || 'No data yet', width / 2, height / 2);
    return { tMin: 0, tMax: 1, fullTMin: 0, fullTMax: 1 };
  }

  let fullTMin = Infinity, fullTMax = -Infinity;
  withPoints.forEach((s) => s.points.forEach((p) => {
    if (p.ms < fullTMin) fullTMin = p.ms;
    if (p.ms > fullTMax) fullTMax = p.ms;
  }));
  if (fullTMin === fullTMax) { fullTMin -= 1000; fullTMax += 1000; }

  const tMin = viewWindow ? viewWindow.min : fullTMin;
  const tMax = viewWindow ? viewWindow.max : fullTMax;

  const plotW = width - PLOT_PADDING.left - PLOT_PADDING.right;
  const plotH = height - PLOT_PADDING.top - PLOT_PADDING.bottom;
  const xForMs = (ms) => PLOT_PADDING.left + ((ms - tMin) / (tMax - tMin)) * plotW;

  // Only points inside the current window count for drawing and for
  // auto-scaling the y-axis - a series with no data in a narrow zoomed
  // window just doesn't draw anything, rather than stretching a
  // flat/misleading line across it.
  const nonEmpty = withPoints
    .map((s) => ({ ...s, visible: s.points.filter((p) => p.ms >= tMin && p.ms <= tMax) }))
    .filter((s) => s.visible.length > 0);

  if (!nonEmpty.length) {
    ctx.fillStyle = getCssVar('--text-dim');
    ctx.font = '13px ' + getCssVar('--sans');
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText('No data in this range', width / 2, height / 2);
    return { tMin, tMax, fullTMin, fullTMax };
  }

  const single = nonEmpty.length === 1;

  nonEmpty.forEach((s) => {
    const values = s.visible.map((p) => p.v);
    let minV = Math.min(...values), maxV = Math.max(...values);
    if (minV === maxV) { minV -= 1; maxV += 1; } else {
      const pad = (maxV - minV) * 0.1;
      minV -= pad; maxV += pad;
    }
    s._minV = minV; s._maxV = maxV;
    s._yForValue = (v) => PLOT_PADDING.top + plotH - ((v - minV) / (maxV - minV)) * plotH;
  });

  // Gridlines - real value labels for a single series, relative % for
  // an overlay (each series' own 0-100%, never a fabricated shared unit).
  const gridLines = 4;
  ctx.strokeStyle = getCssVar('--panel-border');
  ctx.lineWidth = 1;
  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  for (let i = 0; i <= gridLines; i++) {
    const y = Math.round(PLOT_PADDING.top + (plotH * i) / gridLines) + 0.5;
    ctx.beginPath();
    ctx.moveTo(PLOT_PADDING.left, y);
    ctx.lineTo(width - PLOT_PADDING.right, y);
    ctx.stroke();

    const label = single
      ? (nonEmpty[0]._minV + ((nonEmpty[0]._maxV - nonEmpty[0]._minV) * (gridLines - i)) / gridLines).toFixed(nonEmpty[0].decimals ?? 1)
      : Math.round(((gridLines - i) / gridLines) * 100) + '%';
    ctx.fillText(label, PLOT_PADDING.left - 8, y - 0.5);
  }

  // Area fill only makes sense for one series - overlapping washes from
  // several would just muddy the plot.
  if (single) {
    const s = nonEmpty[0];
    ctx.beginPath();
    ctx.moveTo(xForMs(s.visible[0].ms), s._yForValue(s.visible[0].v));
    s.visible.forEach((p) => ctx.lineTo(xForMs(p.ms), s._yForValue(p.v)));
    ctx.lineTo(xForMs(s.visible[s.visible.length - 1].ms), PLOT_PADDING.top + plotH);
    ctx.lineTo(xForMs(s.visible[0].ms), PLOT_PADDING.top + plotH);
    ctx.closePath();
    ctx.fillStyle = hexToRgba(getCssVar('--accent'), 0.1);
    ctx.fill();
  }

  nonEmpty.forEach((s) => {
    const color = single ? getCssVar('--accent') : s.color;

    ctx.beginPath();
    s.visible.forEach((p, i) => {
      const x = xForMs(p.ms), y = s._yForValue(p.v);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.stroke();

    const last = s.visible[s.visible.length - 1];
    const lastX = xForMs(last.ms), lastY = s._yForValue(last.v);
    ctx.beginPath();
    ctx.arc(lastX, lastY, 6, 0, Math.PI * 2);
    ctx.fillStyle = getCssVar('--panel');
    ctx.fill();
    ctx.beginPath();
    ctx.arc(lastX, lastY, 4, 0, Math.PI * 2);
    ctx.fillStyle = color;
    ctx.fill();
  });

  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textBaseline = 'alphabetic';
  ctx.textAlign = 'left';
  ctx.fillText(formatMsAsTime(tMin), PLOT_PADDING.left, height - 6);
  ctx.textAlign = 'right';
  ctx.fillText(formatMsAsTime(tMax), width - PLOT_PADDING.right, height - 6);

  if (hoverMs !== null) {
    const hx = xForMs(hoverMs);
    ctx.beginPath();
    ctx.moveTo(hx, PLOT_PADDING.top);
    ctx.lineTo(hx, PLOT_PADDING.top + plotH);
    ctx.strokeStyle = getCssVar('--text-dim');
    ctx.lineWidth = 1;
    ctx.stroke();

    nonEmpty.forEach((s) => {
      const color = single ? getCssVar('--accent') : s.color;
      const p = nearestPointByTime(s.visible, hoverMs);
      if (!p) return;
      const hy = s._yForValue(p.v);
      ctx.beginPath();
      ctx.arc(xForMs(p.ms), hy, 4, 0, Math.PI * 2);
      ctx.fillStyle = getCssVar('--panel');
      ctx.fill();
      ctx.beginPath();
      ctx.arc(xForMs(p.ms), hy, 3, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    });
  }

  return { tMin, tMax, fullTMin, fullTMax };
}

// zoomResetBtnId is optional - only the Data Viewer's plot wires one up
// (zoom/pan is scoped to that plot, not the live sensor plots, which
// also don't pass a third argument here).
function createStaticPlotController(canvasId, tooltipId, zoomResetBtnId) {
  const canvas = document.getElementById(canvasId);
  const tooltip = document.getElementById(tooltipId);
  const zoomResetBtn = zoomResetBtnId ? document.getElementById(zoomResetBtnId) : null;

  let series = []; // [{column, points: [{t, v, ms}], decimals, color}]
  let hoverMs = null;
  let emptyMessage = 'Pick a numeric column to plot';
  let domain = { tMin: 0, tMax: 1, fullTMin: 0, fullTMax: 1 };

  // null/null = showing the full data extent (not zoomed). Only ever
  // set together.
  let viewMin = null;
  let viewMax = null;
  let isPanning = false;
  let panStartX = 0;
  let panStartMin = 0;
  let panStartMax = 0;

  function isZoomed() {
    return viewMin !== null && viewMax !== null;
  }

  function draw() {
    const viewWindow = isZoomed() ? { min: viewMin, max: viewMax } : null;
    domain = drawOverlayPlot(canvas, series, hoverMs, emptyMessage, viewWindow);
    if (zoomResetBtn) zoomResetBtn.style.display = isZoomed() ? 'block' : 'none';
  }

  // Clamps a candidate [min, max] window to the data's full extent,
  // sliding rather than shrinking it if one edge overflows - so
  // panning/zooming near either end stops cleanly at the data boundary
  // instead of revealing empty space.
  function clampWindow(min, max) {
    const span = max - min;
    if (min < domain.fullTMin) { min = domain.fullTMin; max = min + span; }
    if (max > domain.fullTMax) { max = domain.fullTMax; min = max - span; }
    return {
      min: Math.max(domain.fullTMin, min),
      max: Math.min(domain.fullTMax, max),
    };
  }

  canvas.addEventListener('mousemove', (e) => {
    if (isPanning) return;
    const nonEmpty = series.filter((s) => s.points.length > 0);
    if (!nonEmpty.length) return;

    const rect = canvas.getBoundingClientRect();
    const plotW = rect.width - PLOT_PADDING.left - PLOT_PADDING.right;
    const relX = (e.clientX - rect.left) - PLOT_PADDING.left;
    const frac = Math.max(0, Math.min(1, relX / plotW));
    hoverMs = domain.tMin + frac * (domain.tMax - domain.tMin);
    draw();

    const single = nonEmpty.length === 1;
    tooltip.innerHTML = '';
    nonEmpty.forEach((s) => {
      const p = nearestPointByTime(s.points, hoverMs);
      if (!p) return;
      const row = document.createElement('div');
      row.className = 'tt-row';
      if (!single) {
        const dot = document.createElement('span');
        dot.className = 'tt-dot';
        dot.style.background = s.color;
        row.appendChild(dot);
      }
      const valueEl = document.createElement('span');
      valueEl.className = 'tt-value';
      valueEl.textContent = p.v.toFixed(s.decimals ?? 1);
      row.appendChild(valueEl);
      tooltip.appendChild(row);
    });
    const refPoint = nearestPointByTime(nonEmpty[0].points, hoverMs);
    const timeEl = document.createElement('div');
    timeEl.className = 'tt-time';
    timeEl.textContent = refPoint ? plotTimeOnly(refPoint.t) : '';
    tooltip.appendChild(timeEl);

    tooltip.style.left = (PLOT_PADDING.left + frac * plotW) + 'px';
    tooltip.style.top = '0px';
    tooltip.style.display = 'block';
  });

  canvas.addEventListener('mouseleave', () => {
    if (isPanning) return;
    tooltip.style.display = 'none';
    hoverMs = null;
    draw();
  });

  // Scroll to zoom, centered on the cursor's time position rather than
  // the window center, so zooming in while pointed at a feature keeps
  // that feature under the cursor instead of drifting away.
  canvas.addEventListener('wheel', (e) => {
    if (!series.some((s) => s.points.length > 0)) return;
    e.preventDefault();

    const rect = canvas.getBoundingClientRect();
    const plotW = rect.width - PLOT_PADDING.left - PLOT_PADDING.right;
    const relX = (e.clientX - rect.left) - PLOT_PADDING.left;
    const frac = Math.max(0, Math.min(1, relX / plotW));

    const curMin = isZoomed() ? viewMin : domain.fullTMin;
    const curMax = isZoomed() ? viewMax : domain.fullTMax;
    const anchorMs = curMin + frac * (curMax - curMin);

    const fullSpan = domain.fullTMax - domain.fullTMin;
    const minSpan = Math.max(1000, fullSpan * 0.01); // never zoom in past ~1% of the data or 1s
    const zoomFactor = e.deltaY < 0 ? 0.8 : 1.25;
    const newSpan = Math.max(minSpan, Math.min(fullSpan, (curMax - curMin) * zoomFactor));

    const newMin = anchorMs - (anchorMs - curMin) * (newSpan / (curMax - curMin));
    const clamped = clampWindow(newMin, newMin + newSpan);

    // Snap back to "not zoomed" once the window would cover the whole
    // extent again, so the reset button/state stays exact instead of
    // sitting at a window that's merely very close to full.
    if (newSpan >= fullSpan - 1) {
      viewMin = null;
      viewMax = null;
    } else {
      viewMin = clamped.min;
      viewMax = clamped.max;
    }
    draw();
  }, { passive: false });

  // Click-drag to pan once zoomed in - only meaningful with a window
  // narrower than the full extent, but harmless to allow always (it's a
  // no-op against a window already at the full data span, since
  // clampWindow can't move it anywhere).
  canvas.addEventListener('mousedown', (e) => {
    if (!series.some((s) => s.points.length > 0)) return;
    isPanning = true;
    panStartX = e.clientX;
    panStartMin = isZoomed() ? viewMin : domain.fullTMin;
    panStartMax = isZoomed() ? viewMax : domain.fullTMax;
    tooltip.style.display = 'none';
    canvas.style.cursor = 'grabbing';
  });

  window.addEventListener('mousemove', (e) => {
    if (!isPanning) return;
    const rect = canvas.getBoundingClientRect();
    const plotW = rect.width - PLOT_PADDING.left - PLOT_PADDING.right;
    const span = panStartMax - panStartMin;
    const deltaMs = -((e.clientX - panStartX) / plotW) * span;
    const clamped = clampWindow(panStartMin + deltaMs, panStartMax + deltaMs);
    viewMin = clamped.min;
    viewMax = clamped.max;
    draw();
  });

  window.addEventListener('mouseup', () => {
    if (!isPanning) return;
    isPanning = false;
    canvas.style.cursor = 'grab';
  });

  canvas.addEventListener('dblclick', () => {
    viewMin = null;
    viewMax = null;
    draw();
  });

  return {
    // newSeries: [{column, points: [{t, v}], decimals, color}]
    setSeries(newSeries) {
      series = newSeries.map((s) => ({
        ...s,
        points: s.points
          .map((p) => ({ t: p.t, v: p.v, ms: parseTsMs(p.t) }))
          .filter((p) => p.ms !== null)
          .sort((a, b) => a.ms - b.ms),
      }));
      hoverMs = null;
      viewMin = null;
      viewMax = null;
      emptyMessage = 'Pick a numeric column to plot';
      canvas.style.cursor = series.length ? 'grab' : '';
      draw();
    },
    setEmptyMessage(message) {
      series = [];
      hoverMs = null;
      viewMin = null;
      viewMax = null;
      emptyMessage = message;
      canvas.style.cursor = '';
      draw();
    },
    resetZoom() {
      viewMin = null;
      viewMax = null;
      draw();
    },
    draw,
  };
}


// ---------- Distribution (histogram) and relationship (scatter) plots ----------
// Non-time-series counterparts to the overlay plot above, added for the
// offline exploration view's stats/correlation/histogram/scatter panels.
// No zoom/pan (these summarize a whole selection at once, not a
// timeline to navigate) - just draw + a hover tooltip, so both
// controllers below are much smaller than createStaticPlotController.

function drawEmptyPlotMessage(ctx, width, height, message) {
  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '13px ' + getCssVar('--sans');
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText(message || 'No data', width / 2, height / 2);
}

// edges: [lo, ..., hi] (bin_count + 1 values), counts: [bin_count values].
// hoverIndex (or null) highlights one bar, for the hover tooltip below.
function drawHistogram(canvas, edges, counts, decimals, hoverIndex) {
  const { ctx, width, height } = getCanvasContext(canvas);
  ctx.clearRect(0, 0, width, height);

  if (!edges.length || !counts.length) {
    drawEmptyPlotMessage(ctx, width, height, 'No numeric values found');
    return;
  }

  const plotW = width - PLOT_PADDING.left - PLOT_PADDING.right;
  const plotH = height - PLOT_PADDING.top - PLOT_PADDING.bottom;
  const maxCount = Math.max(...counts);
  const xForEdge = (i) => PLOT_PADDING.left + (i / counts.length) * plotW;
  const yForCount = (c) => PLOT_PADDING.top + plotH - (maxCount ? (c / maxCount) * plotH : 0);

  const gridLines = 4;
  ctx.strokeStyle = getCssVar('--panel-border');
  ctx.lineWidth = 1;
  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  for (let i = 0; i <= gridLines; i++) {
    const y = Math.round(PLOT_PADDING.top + (plotH * i) / gridLines) + 0.5;
    ctx.beginPath();
    ctx.moveTo(PLOT_PADDING.left, y);
    ctx.lineTo(width - PLOT_PADDING.right, y);
    ctx.stroke();
    ctx.fillText(Math.round((maxCount * (gridLines - i)) / gridLines), PLOT_PADDING.left - 8, y - 0.5);
  }

  const barGap = 2; // a visible surface gap between adjacent bars, not a solid block
  const accent = getCssVar('--accent');
  counts.forEach((count, i) => {
    const x0 = xForEdge(i) + barGap / 2;
    const x1 = xForEdge(i + 1) - barGap / 2;
    const y = yForCount(count);
    const hovered = hoverIndex === i;
    ctx.fillStyle = hexToRgba(accent, hovered ? 0.7 : 0.45);
    ctx.fillRect(x0, y, Math.max(0, x1 - x0), PLOT_PADDING.top + plotH - y);
    if (hovered) {
      ctx.strokeStyle = accent;
      ctx.lineWidth = 1.5;
      ctx.strokeRect(x0, y, Math.max(0, x1 - x0), PLOT_PADDING.top + plotH - y);
    }
  });

  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textBaseline = 'alphabetic';
  ctx.textAlign = 'left';
  ctx.fillText(edges[0].toFixed(decimals ?? 1), PLOT_PADDING.left, height - 6);
  ctx.textAlign = 'right';
  ctx.fillText(edges[edges.length - 1].toFixed(decimals ?? 1), width - PLOT_PADDING.right, height - 6);
}

function createHistogramController(canvasId, tooltipId) {
  const canvas = document.getElementById(canvasId);
  const tooltip = document.getElementById(tooltipId);

  let edges = [];
  let counts = [];
  let decimals = 1;
  let emptyMessage = 'Pick a variable to see its distribution';
  let hoverIndex = null;

  function draw() {
    if (!edges.length) {
      const { ctx, width, height } = getCanvasContext(canvas);
      ctx.clearRect(0, 0, width, height);
      drawEmptyPlotMessage(ctx, width, height, emptyMessage);
      return;
    }
    drawHistogram(canvas, edges, counts, decimals, hoverIndex);
  }

  canvas.addEventListener('mousemove', (e) => {
    if (!edges.length) return;
    const rect = canvas.getBoundingClientRect();
    const plotW = rect.width - PLOT_PADDING.left - PLOT_PADDING.right;
    const relX = (e.clientX - rect.left) - PLOT_PADDING.left;
    const frac = Math.max(0, Math.min(0.999999, relX / plotW));
    const idx = Math.floor(frac * counts.length);
    if (idx < 0 || idx >= counts.length) return;
    hoverIndex = idx;
    draw();

    tooltip.innerHTML = `<div class="tt-row"><span class="tt-value">${counts[idx]}</span></div>` +
      `<div class="tt-time">${edges[idx].toFixed(decimals)} – ${edges[idx + 1].toFixed(decimals)}</div>`;
    tooltip.style.left = (PLOT_PADDING.left + ((idx + 0.5) / counts.length) * plotW) + 'px';
    tooltip.style.top = '0px';
    tooltip.style.display = 'block';
  });

  canvas.addEventListener('mouseleave', () => {
    hoverIndex = null;
    tooltip.style.display = 'none';
    draw();
  });

  return {
    // data: {edges, counts, decimals}
    setData(data) {
      edges = data.edges || [];
      counts = data.counts || [];
      decimals = data.decimals ?? 1;
      hoverIndex = null;
      draw();
    },
    setEmptyMessage(message) {
      edges = [];
      counts = [];
      emptyMessage = message;
      draw();
    },
    draw,
  };
}

// points: [{x, y}]. regression: {slope, intercept} or null (shown only
// with 2+ points and non-constant x - see /offline/.../scatter).
function drawScatter(canvas, points, regression, xLabel, yLabel, decimals, hoverPoint) {
  const { ctx, width, height } = getCanvasContext(canvas);
  ctx.clearRect(0, 0, width, height);

  if (!points.length) {
    drawEmptyPlotMessage(ctx, width, height, 'No paired values found');
    return;
  }

  const plotW = width - PLOT_PADDING.left - PLOT_PADDING.right;
  const plotH = height - PLOT_PADDING.top - PLOT_PADDING.bottom;

  let minX = Math.min(...points.map((p) => p.x)), maxX = Math.max(...points.map((p) => p.x));
  let minY = Math.min(...points.map((p) => p.y)), maxY = Math.max(...points.map((p) => p.y));
  if (minX === maxX) { minX -= 1; maxX += 1; } else { const pad = (maxX - minX) * 0.08; minX -= pad; maxX += pad; }
  if (minY === maxY) { minY -= 1; maxY += 1; } else { const pad = (maxY - minY) * 0.08; minY -= pad; maxY += pad; }

  const xFor = (x) => PLOT_PADDING.left + ((x - minX) / (maxX - minX)) * plotW;
  const yFor = (y) => PLOT_PADDING.top + plotH - ((y - minY) / (maxY - minY)) * plotH;

  const gridLines = 4;
  ctx.strokeStyle = getCssVar('--panel-border');
  ctx.lineWidth = 1;
  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  for (let i = 0; i <= gridLines; i++) {
    const y = Math.round(PLOT_PADDING.top + (plotH * i) / gridLines) + 0.5;
    ctx.beginPath();
    ctx.moveTo(PLOT_PADDING.left, y);
    ctx.lineTo(width - PLOT_PADDING.right, y);
    ctx.stroke();
    const label = minY + ((maxY - minY) * (gridLines - i)) / gridLines;
    ctx.fillText(label.toFixed(decimals ?? 1), PLOT_PADDING.left - 8, y - 0.5);
  }

  const accent = getCssVar('--accent');
  points.forEach((p) => {
    ctx.beginPath();
    ctx.arc(xFor(p.x), yFor(p.y), 3, 0, Math.PI * 2);
    ctx.fillStyle = hexToRgba(accent, 0.5);
    ctx.fill();
  });

  // Regression line drawn as a dashed, muted line rather than a strong
  // accent color - distinguishable from the point cloud by dash pattern
  // as well as color, not color alone.
  if (regression) {
    ctx.save();
    ctx.setLineDash([6, 4]);
    ctx.strokeStyle = getCssVar('--text-dim');
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(xFor(minX), yFor(regression.slope * minX + regression.intercept));
    ctx.lineTo(xFor(maxX), yFor(regression.slope * maxX + regression.intercept));
    ctx.stroke();
    ctx.restore();
  }

  if (hoverPoint) {
    ctx.beginPath();
    ctx.arc(xFor(hoverPoint.x), yFor(hoverPoint.y), 5, 0, Math.PI * 2);
    ctx.fillStyle = getCssVar('--panel');
    ctx.fill();
    ctx.beginPath();
    ctx.arc(xFor(hoverPoint.x), yFor(hoverPoint.y), 3.5, 0, Math.PI * 2);
    ctx.fillStyle = accent;
    ctx.fill();
  }

  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textBaseline = 'alphabetic';
  ctx.textAlign = 'left';
  ctx.fillText(minX.toFixed(decimals ?? 1), PLOT_PADDING.left, height - 6);
  ctx.textAlign = 'right';
  ctx.fillText(maxX.toFixed(decimals ?? 1), width - PLOT_PADDING.right, height - 6);
  ctx.textAlign = 'center';
  ctx.fillText(`${xLabel} →`, PLOT_PADDING.left + plotW / 2, height - 6);

  ctx.save();
  ctx.translate(12, PLOT_PADDING.top + plotH / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';
  ctx.fillText(`${yLabel} →`, 0, 0);
  ctx.restore();
}

function createScatterController(canvasId, tooltipId) {
  const canvas = document.getElementById(canvasId);
  const tooltip = document.getElementById(tooltipId);

  let points = [];
  let regression = null;
  let xLabel = 'x', yLabel = 'y', decimals = 1;
  let emptyMessage = 'Pick two variables to compare';
  let hoverPoint = null;

  function draw() {
    if (!points.length) {
      const { ctx, width, height } = getCanvasContext(canvas);
      ctx.clearRect(0, 0, width, height);
      drawEmptyPlotMessage(ctx, width, height, emptyMessage);
      return;
    }
    drawScatter(canvas, points, regression, xLabel, yLabel, decimals, hoverPoint);
  }

  canvas.addEventListener('mousemove', (e) => {
    if (!points.length) return;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left, my = e.clientY - rect.top;

    // Linear nearest-neighbor in pixel space - fine at the point counts
    // a single day's merged CSV produces (thousands, not millions).
    let minX = Math.min(...points.map((p) => p.x)), maxX = Math.max(...points.map((p) => p.x));
    let minY = Math.min(...points.map((p) => p.y)), maxY = Math.max(...points.map((p) => p.y));
    if (minX === maxX) { minX -= 1; maxX += 1; }
    if (minY === maxY) { minY -= 1; maxY += 1; }
    const plotW = rect.width - PLOT_PADDING.left - PLOT_PADDING.right;
    const plotH = rect.height - PLOT_PADDING.top - PLOT_PADDING.bottom;
    const xFor = (x) => PLOT_PADDING.left + ((x - minX) / (maxX - minX)) * plotW;
    const yFor = (y) => PLOT_PADDING.top + plotH - ((y - minY) / (maxY - minY)) * plotH;

    let best = null, bestDist = Infinity;
    points.forEach((p) => {
      const dx = xFor(p.x) - mx, dy = yFor(p.y) - my;
      const d = dx * dx + dy * dy;
      if (d < bestDist) { bestDist = d; best = p; }
    });
    if (!best || bestDist > 400) { // ~20px
      if (hoverPoint) { hoverPoint = null; tooltip.style.display = 'none'; draw(); }
      return;
    }
    hoverPoint = best;
    draw();
    tooltip.innerHTML = `<div class="tt-row"><span class="tt-value">${xLabel}: ${best.x.toFixed(decimals)}</span></div>` +
      `<div class="tt-row"><span class="tt-value">${yLabel}: ${best.y.toFixed(decimals)}</span></div>`;
    tooltip.style.left = xFor(best.x) + 'px';
    tooltip.style.top = '0px';
    tooltip.style.display = 'block';
  });

  canvas.addEventListener('mouseleave', () => {
    hoverPoint = null;
    tooltip.style.display = 'none';
    draw();
  });

  return {
    // data: {points: [{x,y}], regression: {slope,intercept}|null, xLabel, yLabel, decimals}
    setData(data) {
      points = data.points || [];
      regression = data.regression || null;
      xLabel = data.xLabel || 'x';
      yLabel = data.yLabel || 'y';
      decimals = data.decimals ?? 1;
      hoverPoint = null;
      draw();
    },
    setEmptyMessage(message) {
      points = [];
      emptyMessage = message;
      draw();
    },
    draw,
  };
}

// Vertical-profile plot: one or more overlaid series, each drawn as a
// mean line running through its altitude bins (see _bin_altitude_profile
// in app.py) with a shaded mean±std band behind it - the altitude-
// profile panel's replacement for a raw per-point scatter, which at
// ~1000+ points per flight was mostly noise. y (altitude) is the shared
// axis across every series, same as time is the shared axis in
// drawOverlayPlot; x (the instrument value) is real when there's one
// series, or scaled independently per series - each to its own mean±std
// range - when several are overlaid, same "relative when mismatched
// units are stacked together" rule drawOverlayPlot uses.
function drawProfileLines(canvas, series, yLabel, decimals, hoverAltitude) {
  const { ctx, width, height } = getCanvasContext(canvas);
  ctx.clearRect(0, 0, width, height);

  const withBins = series.filter((s) => s.bins.length > 0);
  if (!withBins.length) {
    drawEmptyPlotMessage(ctx, width, height, 'No paired values found');
    return;
  }

  const plotW = width - PLOT_PADDING.left - PLOT_PADDING.right;
  const plotH = height - PLOT_PADDING.top - PLOT_PADDING.bottom;

  let minY = Infinity, maxY = -Infinity;
  withBins.forEach((s) => s.bins.forEach((b) => {
    if (b.altitude < minY) minY = b.altitude;
    if (b.altitude > maxY) maxY = b.altitude;
  }));
  if (minY === maxY) { minY -= 1; maxY += 1; } else { const pad = (maxY - minY) * 0.08; minY -= pad; maxY += pad; }
  const yFor = (y) => PLOT_PADDING.top + plotH - ((y - minY) / (maxY - minY)) * plotH;

  const single = withBins.length === 1;

  withBins.forEach((s) => {
    let minX = Math.min(...s.bins.map((b) => b.mean - b.std));
    let maxX = Math.max(...s.bins.map((b) => b.mean + b.std));
    if (minX === maxX) { minX -= 1; maxX += 1; } else { const pad = (maxX - minX) * 0.08; minX -= pad; maxX += pad; }
    s._minX = minX; s._maxX = maxX;
    s._xFor = (x) => PLOT_PADDING.left + ((x - minX) / (maxX - minX)) * plotW;
  });

  // Altitude gridlines - the shared axis, so always real values (never a
  // fabricated shared unit across series).
  const gridLines = 4;
  ctx.strokeStyle = getCssVar('--panel-border');
  ctx.lineWidth = 1;
  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  for (let i = 0; i <= gridLines; i++) {
    const y = Math.round(PLOT_PADDING.top + (plotH * i) / gridLines) + 0.5;
    ctx.beginPath();
    ctx.moveTo(PLOT_PADDING.left, y);
    ctx.lineTo(width - PLOT_PADDING.right, y);
    ctx.stroke();
    const label = minY + ((maxY - minY) * (gridLines - i)) / gridLines;
    ctx.fillText(label.toFixed(1), PLOT_PADDING.left - 8, y - 0.5);
  }

  withBins.forEach((s) => {
    const color = single ? getCssVar('--accent') : s.color;

    // Shaded mean±std band - one polygon tracing (mean-std, altitude) up
    // through every bin, then back down through (mean+std, altitude).
    // Kept even when several series overlap (unlike drawOverlayPlot,
    // which only fills for a single series) since the band is the whole
    // point of this chart, not just emphasis - each series' own color at
    // low alpha keeps overlapping bands distinguishable.
    ctx.beginPath();
    s.bins.forEach((b, i) => {
      const x = s._xFor(b.mean - b.std), y = yFor(b.altitude);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    for (let i = s.bins.length - 1; i >= 0; i--) {
      ctx.lineTo(s._xFor(s.bins[i].mean + s.bins[i].std), yFor(s.bins[i].altitude));
    }
    ctx.closePath();
    ctx.fillStyle = hexToRgba(color, single ? 0.15 : 0.12);
    ctx.fill();

    // Mean line.
    ctx.beginPath();
    s.bins.forEach((b, i) => {
      const x = s._xFor(b.mean), y = yFor(b.altitude);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.stroke();

    // One dot per bin, on the mean line - hover targets, same idea as
    // drawScatter's per-point dots but one per bin instead of one per
    // raw reading.
    s.bins.forEach((b) => {
      ctx.beginPath();
      ctx.arc(s._xFor(b.mean), yFor(b.altitude), 3, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    });
  });

  // Hover crosshair - a horizontal line at the hovered altitude, with
  // each series' nearest bin marked. Mirrors drawOverlayPlot's vertical
  // hover line, just rotated: there time is the shared axis and the
  // crosshair is vertical; here altitude is the shared axis and it's
  // horizontal.
  if (hoverAltitude !== null) {
    const hy = yFor(hoverAltitude);
    ctx.beginPath();
    ctx.moveTo(PLOT_PADDING.left, hy);
    ctx.lineTo(width - PLOT_PADDING.right, hy);
    ctx.strokeStyle = getCssVar('--text-dim');
    ctx.lineWidth = 1;
    ctx.stroke();

    withBins.forEach((s) => {
      const color = single ? getCssVar('--accent') : s.color;
      let nearest = s.bins[0], bestDist = Infinity;
      s.bins.forEach((b) => {
        const d = Math.abs(b.altitude - hoverAltitude);
        if (d < bestDist) { bestDist = d; nearest = b; }
      });
      const x = s._xFor(nearest.mean), y = yFor(nearest.altitude);
      ctx.beginPath();
      ctx.arc(x, y, 5, 0, Math.PI * 2);
      ctx.fillStyle = getCssVar('--panel');
      ctx.fill();
      ctx.beginPath();
      ctx.arc(x, y, 3.5, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    });
  }

  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textBaseline = 'alphabetic';
  let minLabel, maxLabel, centerLabel;
  if (single) {
    minLabel = withBins[0]._minX.toFixed(decimals ?? 1);
    maxLabel = withBins[0]._maxX.toFixed(decimals ?? 1);
    centerLabel = `${withBins[0].column} (mean ± std) →`;
  } else {
    minLabel = '0%';
    maxLabel = '100%';
    centerLabel = 'mean ± std, per variable →';
  }
  ctx.textAlign = 'left';
  const minLabelW = ctx.measureText(minLabel).width;
  const maxLabelW = ctx.measureText(maxLabel).width;
  const centerLabelW = ctx.measureText(centerLabel).width;

  ctx.fillText(minLabel, PLOT_PADDING.left, height - 6);
  ctx.textAlign = 'right';
  ctx.fillText(maxLabel, width - PLOT_PADDING.right, height - 6);

  // The narrow small-multiple charts this feeds (one per flight, several
  // per row - see the altitude-profile panel) don't have room for all
  // three bottom labels at once - skip the center one rather than let it
  // overlap min/max, since the active variables are already named in the
  // legend above every chart on this page.
  const centerX = PLOT_PADDING.left + plotW / 2;
  const gap = 8;
  if (centerX - centerLabelW / 2 > PLOT_PADDING.left + minLabelW + gap &&
      centerX + centerLabelW / 2 < width - PLOT_PADDING.right - maxLabelW - gap) {
    ctx.textAlign = 'center';
    ctx.fillText(centerLabel, centerX, height - 6);
  }

  ctx.save();
  ctx.translate(12, PLOT_PADDING.top + plotH / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';
  ctx.fillText(`${yLabel} →`, 0, 0);
  ctx.restore();
}

function createProfileController(canvasId, tooltipId) {
  const canvas = document.getElementById(canvasId);
  const tooltip = document.getElementById(tooltipId);

  let series = []; // [{column, color, bins: [{altitude, mean, std, n}]}]
  let yLabel = 'y', decimals = 1;
  let emptyMessage = 'Pick a variable';
  let hoverAltitude = null;

  function draw() {
    const withBins = series.filter((s) => s.bins.length > 0);
    if (!withBins.length) {
      const { ctx, width, height } = getCanvasContext(canvas);
      ctx.clearRect(0, 0, width, height);
      drawEmptyPlotMessage(ctx, width, height, emptyMessage);
      return;
    }
    drawProfileLines(canvas, series, yLabel, decimals, hoverAltitude);
  }

  canvas.addEventListener('mousemove', (e) => {
    const withBins = series.filter((s) => s.bins.length > 0);
    if (!withBins.length) return;
    const rect = canvas.getBoundingClientRect();
    const my = e.clientY - rect.top;

    let minY = Infinity, maxY = -Infinity;
    withBins.forEach((s) => s.bins.forEach((b) => {
      if (b.altitude < minY) minY = b.altitude;
      if (b.altitude > maxY) maxY = b.altitude;
    }));
    if (minY === maxY) { minY -= 1; maxY += 1; }
    const plotH = rect.height - PLOT_PADDING.top - PLOT_PADDING.bottom;

    if (my < PLOT_PADDING.top - 10 || my > PLOT_PADDING.top + plotH + 10) {
      if (hoverAltitude !== null) { hoverAltitude = null; tooltip.style.display = 'none'; draw(); }
      return;
    }

    const rawAltitude = minY + ((PLOT_PADDING.top + plotH - my) / plotH) * (maxY - minY);
    hoverAltitude = Math.max(minY, Math.min(maxY, rawAltitude));
    draw();

    const rows = withBins.map((s) => {
      let nearest = s.bins[0], bestDist = Infinity;
      s.bins.forEach((b) => {
        const d = Math.abs(b.altitude - hoverAltitude);
        if (d < bestDist) { bestDist = d; nearest = b; }
      });
      const color = withBins.length === 1 ? getCssVar('--accent') : s.color;
      const quart = (nearest.median !== undefined)
        ? ` · median ${nearest.median.toFixed(decimals)} [${nearest.p25.toFixed(decimals)}–${nearest.p75.toFixed(decimals)}]`
        : '';
      return `<div class="tt-row"><span class="tt-dot" style="background:${color}"></span>` +
        `<span class="tt-value">${s.column}: ${nearest.mean.toFixed(decimals)} ± ${nearest.std.toFixed(decimals)}${quart} (n=${nearest.n})</span></div>`;
    }).join('');
    const nearestAlt = withBins[0].bins.reduce((best, b) => Math.abs(b.altitude - hoverAltitude) < Math.abs(best - hoverAltitude) ? b.altitude : best, withBins[0].bins[0].altitude);
    tooltip.innerHTML = `<div class="tt-row"><span class="tt-value">${nearestAlt.toFixed(0)} m level</span></div>` + rows;
    tooltip.style.left = (rect.width / 2) + 'px';
    tooltip.style.top = '0px';
    tooltip.style.display = 'block';
  });

  canvas.addEventListener('mouseleave', () => {
    hoverAltitude = null;
    tooltip.style.display = 'none';
    draw();
  });

  return {
    // data: {series: [{column, color, bins}], yLabel, decimals}
    setData(data) {
      series = data.series || [];
      yLabel = data.yLabel || 'y';
      decimals = data.decimals ?? 1;
      hoverAltitude = null;
      draw();
    },
    setEmptyMessage(message) {
      series = [];
      emptyMessage = message;
      draw();
    },
    draw,
  };
}
