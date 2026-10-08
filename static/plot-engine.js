// Shared time-series overlay plotting engine - one or more series drawn
// against a real (not index-based) time x-axis, with scroll-to-zoom,
// drag-to-pan, and double-click-to-reset. Used by both the online Data
// Viewer (templates/index.html, reading over SFTP) and the offline
// merged-data viewer (templates/offline.html, reading local files) -
// the drawing/interaction code is identical either way, only where the
// points come from differs, so it lives here once rather than twice.

const PLOT_PADDING = { top: 16, right: 16, bottom: 24, left: 46 };

// ---------- Units, axis groups, ticks, legend ----------
// The CSVs carry no unit metadata, so the unit is read off the column name
// (the logger's own naming: *_ug_m3, *_ppb, *_C, *_pct, altitude_agl_m, ...).
// Unknown columns get no unit and are labelled by name alone. Order matters:
// the more specific patterns come first.
const COLUMN_UNIT_RULES = [
  [/_ug_m3$/i, 'µg/m³'],
  [/_ppb$/i, 'ppb'],
  [/_ppm$/i, 'ppm'],
  [/(^|_)(UV|blue|green|red|IR)_BC(1|2|c)(_smooth)?$|(^|_)BCc_(WB|FF)$|(^|_)delta_C$/i, 'ng/m³'],
  [/(^|_)(imet_)?temp$|(^|_)hum_temp$/i, '°C ×100'],
  [/_pct$|_percent$|(^|_)rel_hum$|(^|_)Relative_Humidity$|(^|_)sample_RH$/i, '%'],
  [/_C$|(^|_)Temperature$|(^|_)LDTemp$|(^|_)TofP$|(^|_)Temp$/i, '°C'],
  [/_K$/, 'K'],
  [/_Pa$/, 'Pa'],
  [/_mbar$|_mb$/i, 'mbar'],
  [/_torr$/i, 'torr'],
  [/_hPa$|(^|_)pressure$|(^|_)Pressure$|(^|_)P$/i, 'hPa'],
  [/_um2_cm3$/i, 'µm²/cm³'],
  [/_cm3$|(^|_)PartCon$/i, '#/cm³'],
  [/(^|_)POPS_Flow$/i, 'cm³/s'],
  [/_lpm$/i, 'L/min'],
  [/_ms$|(^|_)Wind_Speed$|(^|_)gps_speed$|(^|_)(U|V|W)_Vector$/i, 'm/s'],
  [/_deg$|(^|_)Wind_Direction$|(^|_)Compass_Heading$|(^|_)Pitch$|(^|_)Roll$|(^|_)yaw$/i, '°'],
  [/_mm$/i, 'mm'],
  [/_nm$/i, 'nm'],
  [/_m$|(^|_)altitude$|(^|_)Altitude$/i, 'm'],
  [/_mA$/, 'mA'],
  [/_nA$/, 'nA'],
  [/_V$/, 'V'],
  [/_hrs$/i, 'h'],
  [/_s$/, 's'],
  [/(^|_)AAE/i, ''],
  [/(^|_)PartCt$|(^|_)HistSum$|(^|_)b\d+$/i, 'counts'],
];

function unitOfColumn(column) {
  const name = String(column || '');
  for (const [re, unit] of COLUMN_UNIT_RULES) {
    if (re.test(name)) return unit;
  }
  return '';
}

function labelWithUnit(column) {
  const unit = unitOfColumn(column);
  return unit ? `${column} (${unit})` : String(column);
}

// Round tick positions: 1/2/5 x 10^k steps, at most maxTicks of them, with
// the number of decimals the step needs.
function niceTicks(min, max, maxTicks) {
  const range = max - min;
  if (!(range > 0)) return { ticks: [min], decimals: 1 };
  const rawStep = range / Math.max(1, maxTicks || 5);
  const mag = Math.pow(10, Math.floor(Math.log10(rawStep)));
  const norm = rawStep / mag;
  const step = (norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10) * mag;
  const decimals = Math.max(0, -Math.floor(Math.log10(step)));
  const ticks = [];
  for (let v = Math.ceil(min / step) * step; v <= max + step * 1e-6; v += step) {
    ticks.push(Number(v.toFixed(decimals)));
  }
  return { ticks, decimals };
}

function paddedRange(lo, hi, frac) {
  if (!(hi > lo)) return [lo - 1, hi + 1];
  const pad = (hi - lo) * (frac === undefined ? 0.08 : frac);
  return [lo - pad, hi + pad];
}

// Groups series by unit so every variable is drawn on a real axis: all the
// series sharing a unit share one axis and one range; each further unit
// gets its own axis. rangeOf(series) -> [lo, hi] of what that series draws.
// A group with one series takes that series' colour, so axis labels say
// which line they belong to; a shared group uses the neutral text colour.
function axisGroupsFor(seriesList, rangeOf, singleColor, maxTicks) {
  const groups = [];
  const byKey = new Map();
  seriesList.forEach((s, idx) => {
    const unit = unitOfColumn(s.column);
    const key = unit || ('col:' + s.column);
    let g = byKey.get(key);
    if (!g) {
      g = { unit, label: unit || s.column, indices: [], lo: Infinity, hi: -Infinity };
      byKey.set(key, g);
      groups.push(g);
    }
    const [lo, hi] = rangeOf(s);
    if (lo < g.lo) g.lo = lo;
    if (hi > g.hi) g.hi = hi;
    g.indices.push(idx);
  });
  groups.forEach((g) => {
    // The axis runs from the round tick at or just below the data's low end
    // to the one at or just above its high end, so the data spreads over
    // the whole axis and both ends are labelled.
    const [lo, hi] = paddedRange(g.lo, g.hi, 0.03);
    const t = niceTicks(lo, hi, maxTicks || 5);
    const step = t.ticks.length > 1 ? t.ticks[1] - t.ticks[0] : (hi - lo) || 1;
    g.min = Math.floor(lo / step + 1e-9) * step;
    g.max = Math.ceil(hi / step - 1e-9) * step;
    if (g.max <= g.min) g.max = g.min + step;
    g.decimals = t.decimals;
    g.ticks = [];
    for (let v = g.min; v <= g.max + step * 1e-6; v += step) g.ticks.push(Number(v.toFixed(t.decimals)));
    g.color = g.indices.length === 1
      ? (seriesList.length === 1 ? singleColor : (seriesList[g.indices[0]].color || singleColor))
      : getCssVar('--text-dim');
    g.indices.forEach((i) => { seriesList[i]._group = g; });
  });
  return groups;
}

// The legend lives in a band ABOVE the plot area (never over the data):
// swatch + "column (unit)" per entry, laid out left to right and wrapped
// into as many rows as the width needs, plus an optional muted note. Each
// chart measures the band first (measureOnly) to know where its plot area
// starts, then draws it. Returns the band height in CSS pixels.
const LEGEND_LINE_H = 16;

function drawLegendBand(ctx, entries, x, y, width, note, measureOnly) {
  if (!entries.length && !note) return 0;
  ctx.save();
  ctx.font = '11px ' + getCssVar('--sans');
  const sw = 10, gapX = 14;
  const fit = (label, avail) => {
    let t = label;
    while (t.length > 4 && ctx.measureText(t).width > avail) t = t.slice(0, -2) + '…';
    return t;
  };
  const items = entries.map((e) => {
    const label = fit(e.label, width - sw - 5);
    return { color: e.color, label, w: sw + 5 + ctx.measureText(label).width };
  });
  if (note) {
    const label = fit(note, width);
    items.push({ note: true, label, w: ctx.measureText(label).width });
  }
  const rows = [[]];
  let used = 0;
  items.forEach((it) => {
    if (used > 0 && used + it.w > width) { rows.push([]); used = 0; }
    rows[rows.length - 1].push(it);
    used += it.w + gapX;
  });
  const h = rows.length * LEGEND_LINE_H;
  if (!measureOnly) {
    ctx.textBaseline = 'middle';
    ctx.textAlign = 'left';
    rows.forEach((row, ri) => {
      let cx = x;
      const cy = y + ri * LEGEND_LINE_H + LEGEND_LINE_H / 2;
      row.forEach((it) => {
        if (it.note) {
          ctx.fillStyle = getCssVar('--text-dim');
          ctx.fillText(it.label, cx, cy);
        } else {
          ctx.fillStyle = it.color;
          ctx.fillRect(cx, cy - sw / 2, sw, sw);
          ctx.fillStyle = getCssVar('--text');
          ctx.fillText(it.label, cx + sw + 5, cy);
        }
        cx += it.w + gapX;
      });
    });
  }
  ctx.restore();
  return h;
}

// Top of the plot area below a legend band of height bandH (CSS px).
function plotTopBelowLegend(bandH) {
  return bandH ? 6 + bandH + 6 : PLOT_PADDING.top;
}

// Word-wraps text to maxWidth with the context's current font.
function wrapTextLines(ctx, text, maxWidth) {
  const words = String(text).split(/\s+/);
  const lines = [];
  let cur = '';
  words.forEach((w) => {
    const trial = cur ? cur + ' ' + w : w;
    if (ctx.measureText(trial).width <= maxWidth || !cur) cur = trial; else { lines.push(cur); cur = w; }
  });
  if (cur) lines.push(cur);
  return lines;
}

// (kept for callers that still want a boxed legend inside a plot)
function drawLegend(ctx, entries, x, y, maxWidth) {
  if (!entries.length) return 0;
  const font = '11px ' + getCssVar('--sans');
  ctx.save();
  ctx.font = font;
  const lineH = 15;
  const sw = 10;
  let w = 0;
  entries.forEach((e) => { w = Math.max(w, ctx.measureText(e.label).width); });
  const boxW = Math.min(maxWidth || Infinity, w + sw + 22);
  const boxH = entries.length * lineH + 8;
  ctx.fillStyle = hexToRgba(getCssVar('--panel') || '#ffffff', 0.85);
  ctx.strokeStyle = getCssVar('--panel-border');
  ctx.lineWidth = 1;
  ctx.fillRect(x, y, boxW, boxH);
  ctx.strokeRect(x + 0.5, y + 0.5, boxW - 1, boxH - 1);
  ctx.textBaseline = 'middle';
  ctx.textAlign = 'left';
  entries.forEach((e, i) => {
    const cy = y + 4 + lineH * i + lineH / 2;
    ctx.fillStyle = e.color;
    ctx.fillRect(x + 7, cy - sw / 2, sw, sw);
    ctx.fillStyle = getCssVar('--text');
    let label = e.label;
    const avail = boxW - sw - 22;
    while (label.length > 4 && ctx.measureText(label).width > avail) label = label.slice(0, -2) + '…';
    ctx.fillText(label, x + 7 + sw + 6, cy);
  });
  ctx.restore();
  return boxH;
}


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
// Time-series overlay: every series on a real axis. Series sharing a unit
// share one axis; the first unit's axis is on the left, each further unit
// gets its own axis on the right (46 px each). A legend names each line
// with its unit so an exported image stands on its own.
const EXTRA_AXIS_W = 50;

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

  // Only points inside the current window count for drawing and for
  // auto-scaling the axes - a series with no data in a narrow zoomed
  // window just doesn't draw anything.
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
  const accent = getCssVar('--accent');
  const groups = axisGroupsFor(nonEmpty, (s) => {
    const values = s.visible.map((p) => p.v);
    return [Math.min(...values), Math.max(...values)];
  }, accent);

  const rightPad = PLOT_PADDING.right + Math.max(0, groups.length - 1) * EXTRA_AXIS_W;
  const plotW = width - PLOT_PADDING.left - rightPad;
  const legendEntries = nonEmpty.map((s) => ({ color: single ? accent : s.color, label: labelWithUnit(s.column) }));
  const bandW = width - PLOT_PADDING.left - PLOT_PADDING.right;
  const bandH = drawLegendBand(ctx, legendEntries, PLOT_PADDING.left, 6, bandW, '', true);
  const topPad = plotTopBelowLegend(bandH);
  const plotH = height - topPad - PLOT_PADDING.bottom;
  const xForMs = (ms) => PLOT_PADDING.left + ((ms - tMin) / (tMax - tMin)) * plotW;
  groups.forEach((g) => { g.yFor = (v) => topPad + plotH - ((v - g.min) / (g.max - g.min)) * plotH; });
  nonEmpty.forEach((s) => { s._yForValue = s._group.yFor; });

  // Gridlines follow the first axis; every axis gets its own tick labels.
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textBaseline = 'middle';
  groups.forEach((g, gi) => {
    const ticks = g.ticks;
    const dec = Math.max(g.decimals, gi === 0 && single ? (nonEmpty[0].decimals ?? 0) : 0);
    ticks.forEach((v, ti) => {
      const y = Math.round(g.yFor(v)) + 0.5;
      if (gi === 0) {
        ctx.strokeStyle = getCssVar('--panel-border');
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(PLOT_PADDING.left, y);
        ctx.lineTo(PLOT_PADDING.left + plotW, y);
        ctx.stroke();
      }
      ctx.fillStyle = g.color;
      const text = v.toFixed(Math.min(dec, 4)) + (ti === ticks.length - 1 && g.unit ? ' ' + g.unit : '');
      if (gi === 0) {
        ctx.textAlign = 'right';
        ctx.fillText(text, PLOT_PADDING.left - 8, y - 0.5);
      } else {
        ctx.textAlign = 'left';
        ctx.fillText(text, PLOT_PADDING.left + plotW + 8 + (gi - 1) * EXTRA_AXIS_W, y - 0.5);
      }
    });
    if (gi > 0) {
      const ax = Math.round(PLOT_PADDING.left + plotW + (gi - 1) * EXTRA_AXIS_W) + 0.5;
      ctx.strokeStyle = g.color;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(ax, topPad);
      ctx.lineTo(ax, topPad + plotH);
      ctx.stroke();
    }
  });

  if (single) {
    const s = nonEmpty[0];
    ctx.beginPath();
    ctx.moveTo(xForMs(s.visible[0].ms), s._yForValue(s.visible[0].v));
    s.visible.forEach((p) => ctx.lineTo(xForMs(p.ms), s._yForValue(p.v)));
    ctx.lineTo(xForMs(s.visible[s.visible.length - 1].ms), topPad + plotH);
    ctx.lineTo(xForMs(s.visible[0].ms), topPad + plotH);
    ctx.closePath();
    ctx.fillStyle = hexToRgba(accent, 0.1);
    ctx.fill();
  }

  nonEmpty.forEach((s) => {
    const color = single ? accent : s.color;
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
    ctx.beginPath(); ctx.arc(lastX, lastY, 6, 0, Math.PI * 2); ctx.fillStyle = getCssVar('--panel'); ctx.fill();
    ctx.beginPath(); ctx.arc(lastX, lastY, 4, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill();
  });

  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textBaseline = 'alphabetic';
  ctx.textAlign = 'left';
  ctx.fillText(formatMsAsTime(tMin), PLOT_PADDING.left, height - 6);
  ctx.textAlign = 'right';
  ctx.fillText(formatMsAsTime(tMax), PLOT_PADDING.left + plotW, height - 6);
  ctx.textAlign = 'center';
  ctx.fillText('time →', PLOT_PADDING.left + plotW / 2, height - 6);

  drawLegendBand(ctx, legendEntries, PLOT_PADDING.left, 6, bandW, '', false);

  if (hoverMs !== null) {
    const hx = xForMs(hoverMs);
    ctx.beginPath();
    ctx.moveTo(hx, topPad);
    ctx.lineTo(hx, topPad + plotH);
    ctx.strokeStyle = getCssVar('--text-dim');
    ctx.lineWidth = 1;
    ctx.stroke();
    nonEmpty.forEach((s) => {
      const color = single ? accent : s.color;
      const p = nearestPointByTime(s.visible, hoverMs);
      if (!p) return;
      const hy = s._yForValue(p.v);
      ctx.beginPath(); ctx.arc(xForMs(p.ms), hy, 4, 0, Math.PI * 2); ctx.fillStyle = getCssVar('--panel'); ctx.fill();
      ctx.beginPath(); ctx.arc(xForMs(p.ms), hy, 3, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill();
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
    getSeries() { return series; },
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
function drawHistogram(canvas, edges, counts, decimals, hoverIndex, label) {
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

  ctx.strokeStyle = getCssVar('--panel-border');
  ctx.lineWidth = 1;
  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  const { ticks } = niceTicks(0, maxCount || 1, 5);
  ticks.forEach((c, i) => {
    const y = Math.round(yForCount(c)) + 0.5;
    ctx.beginPath();
    ctx.moveTo(PLOT_PADDING.left, y);
    ctx.lineTo(width - PLOT_PADDING.right, y);
    ctx.stroke();
    ctx.fillText(String(Math.round(c)) + (i === ticks.length - 1 ? ' n' : ''), PLOT_PADDING.left - 8, y - 0.5);
  });

  const barGap = 2;
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
  if (label) {
    ctx.textAlign = 'center';
    ctx.fillText(`${labelWithUnit(label)} →`, PLOT_PADDING.left + plotW / 2, height - 6);
  }
  ctx.save();
  ctx.translate(12, PLOT_PADDING.top + plotH / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.textAlign = 'center';
  ctx.fillText('count →', 0, 0);
  ctx.restore();
}

function createHistogramController(canvasId, tooltipId) {
  const canvas = document.getElementById(canvasId);
  const tooltip = document.getElementById(tooltipId);

  let edges = [];
  let counts = [];
  let decimals = 1;
  let label = '';
  let emptyMessage = 'Pick a variable to see its distribution';
  let hoverIndex = null;

  function draw() {
    if (!edges.length) {
      const { ctx, width, height } = getCanvasContext(canvas);
      ctx.clearRect(0, 0, width, height);
      drawEmptyPlotMessage(ctx, width, height, emptyMessage);
      return;
    }
    drawHistogram(canvas, edges, counts, decimals, hoverIndex, label);
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
      label = data.label || '';
      hoverIndex = null;
      draw();
    },
    setEmptyMessage(message) {
      edges = [];
      counts = [];
      emptyMessage = message;
      draw();
    },
    getData() { return { edges, counts }; },
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

  ctx.strokeStyle = getCssVar('--panel-border');
  ctx.lineWidth = 1;
  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  const yt = niceTicks(minY, maxY, 5);
  yt.ticks.forEach((v) => {
    const y = Math.round(yFor(v)) + 0.5;
    ctx.beginPath();
    ctx.moveTo(PLOT_PADDING.left, y);
    ctx.lineTo(width - PLOT_PADDING.right, y);
    ctx.stroke();
    ctx.fillText(v.toFixed(Math.min(yt.decimals, 4)), PLOT_PADDING.left - 8, y - 0.5);
  });

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
  ctx.fillText(`${labelWithUnit(xLabel)} →`, PLOT_PADDING.left + plotW / 2, height - 6);

  ctx.save();
  ctx.translate(12, PLOT_PADDING.top + plotH / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';
  ctx.fillText(`${labelWithUnit(yLabel)} →`, 0, 0);
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
    getData() { return { points, regression, xLabel, yLabel }; },
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
// The altitude axis of a vertical profile runs on fixed 20 m ticks (0, 20,
// 40 ... 200) matching the hover levels, from the lowest to the highest
// bin rounded out to the tick; a very tall range steps up to 50 or 100 m
// so the labels stay readable. Shared by the drawing and the hover.
const PROFILE_TICK_M = 20;

function profileAltitudeAxis(series) {
  let lo = Infinity, hi = -Infinity;
  series.forEach((s) => s.bins.forEach((b) => {
    if (b.altitude < lo) lo = b.altitude;
    if (b.altitude > hi) hi = b.altitude;
  }));
  let step = PROFILE_TICK_M;
  let minY = Math.floor(lo / step) * step;
  let maxY = Math.ceil(hi / step) * step;
  if (maxY <= minY) maxY = minY + step;
  while ((maxY - minY) / step > 14) {
    step = step === 20 ? 50 : step * 2;
    minY = Math.floor(lo / step) * step;
    maxY = Math.ceil(hi / step) * step;
  }
  return { minY, maxY, step };
}

// Vertical profile: altitude (m) is the shared vertical axis on fixed 20 m
// ticks; the instrument values run horizontally on real axes - one per
// unit, the first along the bottom, the next along the top, any further
// ones as extra rows above. A legend names each series with its unit.
const PROFILE_TOP_AXIS_H = 18;

// Where a vertical profile's plot area starts: below the legend band and
// below one row per extra value axis. Shared by the drawing and the hover.
function profileTopOffset(bandH, nUnits) {
  return plotTopBelowLegend(bandH) + Math.max(0, nUnits - 1) * PROFILE_TOP_AXIS_H;
}

function profileLegendEntries(withBins, single, accent) {
  return withBins.map((s) => ({ color: single ? accent : s.color, label: labelWithUnit(s.column) }));
}

function drawProfileLines(canvas, series, yLabel, decimals, hoverAltitude) {
  const { ctx, width, height } = getCanvasContext(canvas);
  ctx.clearRect(0, 0, width, height);

  const withBins = series.filter((s) => s.bins.length > 0);
  if (!withBins.length) {
    drawEmptyPlotMessage(ctx, width, height, 'No paired values found');
    return;
  }

  const single = withBins.length === 1;
  const accent = getCssVar('--accent');
  const groups = axisGroupsFor(withBins, (s) => [
    Math.min(...s.bins.map((b) => b.mean - b.std)),
    Math.max(...s.bins.map((b) => b.mean + b.std)),
  ], accent, (width - PLOT_PADDING.left - PLOT_PADDING.right) < 300 ? 3 : 5);

  const plotW = width - PLOT_PADDING.left - PLOT_PADDING.right;
  const legendEntries = profileLegendEntries(withBins, single, accent);
  const bandH = drawLegendBand(ctx, legendEntries, PLOT_PADDING.left, 6, plotW, 'band = ±1 std', true);
  const topPad = profileTopOffset(bandH, groups.length);
  const plotH = height - topPad - PLOT_PADDING.bottom;

  const { minY, maxY, step } = profileAltitudeAxis(withBins);
  const yFor = (y) => topPad + plotH - ((y - minY) / (maxY - minY)) * plotH;
  groups.forEach((g) => { g.xFor = (x) => PLOT_PADDING.left + ((x - g.min) / (g.max - g.min)) * plotW; });
  withBins.forEach((s) => { s._xFor = s._group.xFor; s._minX = s._group.min; s._maxX = s._group.max; });

  // altitude gridlines + labels (m)
  ctx.strokeStyle = getCssVar('--panel-border');
  ctx.lineWidth = 1;
  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  for (let v = minY; v <= maxY + 1e-9; v += step) {
    const y = Math.round(yFor(v)) + 0.5;
    ctx.beginPath();
    ctx.moveTo(PLOT_PADDING.left, y);
    ctx.lineTo(width - PLOT_PADDING.right, y);
    ctx.stroke();
    ctx.fillText(String(Math.round(v)), PLOT_PADDING.left - 8, y - 0.5);
  }

  // value axes: group 0 along the bottom, others along the top
  ctx.textBaseline = 'alphabetic';
  groups.forEach((g, gi) => {
    const ticks = g.ticks, dec = g.decimals;
    ctx.fillStyle = g.color;
    const yText = gi === 0 ? height - 6 : topPad - 6 - (gi - 1) * PROFILE_TOP_AXIS_H;
    // The unit goes after the last tick only if it does not run into the
    // previous tick's label on this (often narrow) chart; the legend
    // carries the unit anyway.
    const labels = ticks.map((v) => v.toFixed(Math.min(dec, 4)));
    let unitText = g.unit ? ' ' + g.unit : '';
    if (unitText && ticks.length >= 2) {
      const n = ticks.length;
      const gap = g.xFor(ticks[n - 1]) - g.xFor(ticks[n - 2]);
      const need = ctx.measureText(labels[n - 1] + unitText).width + ctx.measureText(labels[n - 2]).width / 2 + 6;
      if (gap < need) unitText = '';
    }
    ticks.forEach((v, ti) => {
      const x = g.xFor(v);
      const isLast = ti === ticks.length - 1;
      ctx.textAlign = ti === 0 ? 'left' : isLast ? 'right' : 'center';
      const tx = ti === 0 ? Math.max(x, PLOT_PADDING.left) : isLast ? Math.min(x, width - PLOT_PADDING.right) : x;
      ctx.fillText(labels[ti] + (isLast ? unitText : ''), tx, yText);
      ctx.beginPath();
      const y0 = gi === 0 ? topPad + plotH : topPad;
      ctx.moveTo(Math.round(x) + 0.5, y0);
      ctx.lineTo(Math.round(x) + 0.5, y0 + (gi === 0 ? 4 : -4));
      ctx.strokeStyle = g.color;
      ctx.stroke();
    });
  });

  withBins.forEach((s) => {
    const color = single ? accent : s.color;
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

    ctx.beginPath();
    s.bins.forEach((b, i) => {
      const x = s._xFor(b.mean), y = yFor(b.altitude);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.stroke();

    s.bins.forEach((b) => {
      ctx.beginPath();
      ctx.arc(s._xFor(b.mean), yFor(b.altitude), 3, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    });
  });

  if (hoverAltitude !== null) {
    const hy = yFor(hoverAltitude);
    ctx.beginPath();
    ctx.moveTo(PLOT_PADDING.left, hy);
    ctx.lineTo(width - PLOT_PADDING.right, hy);
    ctx.strokeStyle = getCssVar('--text-dim');
    ctx.lineWidth = 1;
    ctx.stroke();
    withBins.forEach((s) => {
      const color = single ? accent : s.color;
      let nearest = s.bins[0], bestDist = Infinity;
      s.bins.forEach((b) => {
        const d = Math.abs(b.altitude - hoverAltitude);
        if (d < bestDist) { bestDist = d; nearest = b; }
      });
      const x = s._xFor(nearest.mean), y = yFor(nearest.altitude);
      ctx.beginPath(); ctx.arc(x, y, 5, 0, Math.PI * 2); ctx.fillStyle = getCssVar('--panel'); ctx.fill();
      ctx.beginPath(); ctx.arc(x, y, 3.5, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill();
    });
  }

  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.save();
  ctx.translate(12, topPad + plotH / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';
  ctx.fillText(`${yLabel} (m) →`, 0, 0);
  ctx.restore();

  drawLegendBand(ctx, legendEntries, PLOT_PADDING.left, 6, plotW, 'band = ±1 std', false);
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

    const { minY, maxY } = profileAltitudeAxis(withBins);
    const nUnits = new Set(withBins.map((s) => unitOfColumn(s.column) || ('col:' + s.column))).size;
    const mctx = canvas.getContext('2d');
    const bandH = drawLegendBand(mctx, profileLegendEntries(withBins, withBins.length === 1, '#000'),
      PLOT_PADDING.left, 6, rect.width - PLOT_PADDING.left - PLOT_PADDING.right, 'band = ±1 std', true);
    const topPad = profileTopOffset(bandH, nUnits);
    const plotH = rect.height - topPad - PLOT_PADDING.bottom;

    if (my < topPad - 10 || my > topPad + plotH + 10) {
      if (hoverAltitude !== null) { hoverAltitude = null; tooltip.style.display = 'none'; draw(); }
      return;
    }

    const rawAltitude = minY + ((topPad + plotH - my) / plotH) * (maxY - minY);
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
    getSeries() { return series; },
    draw,
  };
}


// ---------- Linear route: binned instrument value against distance from launch ----------
// The horizontal twin of drawProfileLines: distance from the launch point is
// the shared x axis (0 at the left, the far point at the right - the drone
// flies right-to-left on the inbound run), the instrument value is y - real
// units for a single series, each series scaled to its own mean±std range
// when several are overlaid. Bins carry {distance, from, to, mean, std,
// median, p25, p75, n} (see _bin_by_distance in app.py).
function drawTransectLines(canvas, series, xLabel, decimals, hoverDistance, xMax) {
  const { ctx, width, height } = getCanvasContext(canvas);
  ctx.clearRect(0, 0, width, height);

  const withBins = series.filter((s) => s.bins.length > 0);
  if (!withBins.length) {
    drawEmptyPlotMessage(ctx, width, height, 'No paired values found');
    return;
  }

  const single = withBins.length === 1;
  const accent = getCssVar('--accent');
  const groups = axisGroupsFor(withBins, (s) => [
    Math.min(...s.bins.map((b) => b.mean - b.std)),
    Math.max(...s.bins.map((b) => b.mean + b.std)),
  ], accent);

  const rightPad = PLOT_PADDING.right + Math.max(0, groups.length - 1) * EXTRA_AXIS_W;
  const plotW = width - PLOT_PADDING.left - rightPad;
  const legendEntries = withBins.map((s) => ({ color: single ? accent : s.color, label: labelWithUnit(s.column) }));
  const bandW = width - PLOT_PADDING.left - PLOT_PADDING.right;
  const bandH = drawLegendBand(ctx, legendEntries, PLOT_PADDING.left, 6, bandW, 'band = ±1 std', true);
  const topPad = plotTopBelowLegend(bandH);
  const plotH = height - topPad - PLOT_PADDING.bottom;

  let minX = 0;
  let maxX = xMax || 0;
  withBins.forEach((s) => s.bins.forEach((b) => { if (b.to > maxX) maxX = b.to; }));
  if (maxX <= minX) maxX = minX + 1;
  const xFor = (x) => PLOT_PADDING.left + ((x - minX) / (maxX - minX)) * plotW;
  groups.forEach((g) => { g.yFor = (v) => topPad + plotH - ((v - g.min) / (g.max - g.min)) * plotH; });
  withBins.forEach((s) => { s._yFor = s._group.yFor; s._minY = s._group.min; s._maxY = s._group.max; });

  // distance gridlines (shared axis, real metres)
  const xt = niceTicks(minX, maxX, 6);
  ctx.strokeStyle = getCssVar('--panel-border');
  ctx.lineWidth = 1;
  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';
  xt.ticks.forEach((xv) => {
    const x = Math.round(xFor(xv)) + 0.5;
    ctx.beginPath();
    ctx.moveTo(x, topPad);
    ctx.lineTo(x, topPad + plotH);
    ctx.stroke();
    ctx.fillText(xv.toFixed(0), x, height - 6);
  });

  // value axes: group 0 on the left, others on the right
  ctx.textBaseline = 'middle';
  groups.forEach((g, gi) => {
    const ticks = g.ticks, dec = g.decimals;
    ctx.fillStyle = g.color;
    ticks.forEach((v, ti) => {
      const y = Math.round(g.yFor(v)) + 0.5;
      const text = v.toFixed(Math.min(dec, 4)) + (ti === ticks.length - 1 && g.unit ? ' ' + g.unit : '');
      if (gi === 0) {
        ctx.textAlign = 'right';
        ctx.fillText(text, PLOT_PADDING.left - 8, y - 0.5);
      } else {
        ctx.textAlign = 'left';
        ctx.fillText(text, PLOT_PADDING.left + plotW + 8 + (gi - 1) * EXTRA_AXIS_W, y - 0.5);
      }
    });
    if (gi > 0) {
      const ax = Math.round(PLOT_PADDING.left + plotW + (gi - 1) * EXTRA_AXIS_W) + 0.5;
      ctx.strokeStyle = g.color;
      ctx.beginPath();
      ctx.moveTo(ax, topPad);
      ctx.lineTo(ax, topPad + plotH);
      ctx.stroke();
    }
  });

  withBins.forEach((s) => {
    const color = single ? accent : s.color;
    ctx.beginPath();
    s.bins.forEach((b, i) => {
      const x = xFor(b.distance), y = s._yFor(b.mean - b.std);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    for (let i = s.bins.length - 1; i >= 0; i--) {
      ctx.lineTo(xFor(s.bins[i].distance), s._yFor(s.bins[i].mean + s.bins[i].std));
    }
    ctx.closePath();
    ctx.fillStyle = hexToRgba(color, single ? 0.15 : 0.12);
    ctx.fill();

    ctx.beginPath();
    s.bins.forEach((b, i) => {
      const x = xFor(b.distance), y = s._yFor(b.mean);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.stroke();

    s.bins.forEach((b) => {
      ctx.beginPath();
      ctx.arc(xFor(b.distance), s._yFor(b.mean), 3, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    });
  });

  if (hoverDistance !== null) {
    const hx = xFor(hoverDistance);
    ctx.beginPath();
    ctx.moveTo(hx, topPad);
    ctx.lineTo(hx, topPad + plotH);
    ctx.strokeStyle = getCssVar('--text-dim');
    ctx.lineWidth = 1;
    ctx.stroke();
    withBins.forEach((s) => {
      const color = single ? accent : s.color;
      let nearest = s.bins[0], bestDist = Infinity;
      s.bins.forEach((b) => {
        const d = Math.abs(b.distance - hoverDistance);
        if (d < bestDist) { bestDist = d; nearest = b; }
      });
      const x = xFor(nearest.distance), y = s._yFor(nearest.mean);
      ctx.beginPath(); ctx.arc(x, y, 5, 0, Math.PI * 2); ctx.fillStyle = getCssVar('--panel'); ctx.fill();
      ctx.beginPath(); ctx.arc(x, y, 3.5, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill();
    });
  }

  ctx.fillStyle = getCssVar('--text-dim');
  ctx.font = '11px ' + getCssVar('--mono');
  ctx.textBaseline = 'alphabetic';
  ctx.textAlign = 'center';
  ctx.fillText(`← ${xLabel} →`, PLOT_PADDING.left + plotW / 2, height - 18);

  drawLegendBand(ctx, legendEntries, PLOT_PADDING.left, 6, bandW, 'band = ±1 std', false);
}

function createTransectController(canvasId, tooltipId) {
  const canvas = document.getElementById(canvasId);
  const tooltip = document.getElementById(tooltipId);

  let series = [];
  let xLabel = 'distance (m)', decimals = 1, xMax = 0;
  let emptyMessage = 'Pick a variable';
  let hoverDistance = null;

  function draw() {
    const withBins = series.filter((s) => s.bins.length > 0);
    if (!withBins.length) {
      const { ctx, width, height } = getCanvasContext(canvas);
      ctx.clearRect(0, 0, width, height);
      drawEmptyPlotMessage(ctx, width, height, emptyMessage);
      return;
    }
    drawTransectLines(canvas, series, xLabel, decimals, hoverDistance, xMax);
  }

  canvas.addEventListener('mousemove', (e) => {
    const withBins = series.filter((s) => s.bins.length > 0);
    if (!withBins.length) return;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    let maxX = xMax || 0;
    withBins.forEach((s) => s.bins.forEach((b) => { if (b.to > maxX) maxX = b.to; }));
    if (maxX <= 0) maxX = 1;
    const nUnits = new Set(withBins.map((s) => unitOfColumn(s.column) || ('col:' + s.column))).size;
    const plotW = rect.width - PLOT_PADDING.left - PLOT_PADDING.right - Math.max(0, nUnits - 1) * EXTRA_AXIS_W;
    if (mx < PLOT_PADDING.left - 10 || mx > PLOT_PADDING.left + plotW + 10) {
      if (hoverDistance !== null) { hoverDistance = null; tooltip.style.display = 'none'; draw(); }
      return;
    }
    hoverDistance = Math.max(0, Math.min(maxX, ((mx - PLOT_PADDING.left) / plotW) * maxX));
    draw();

    const rows = withBins.map((s) => {
      let nearest = s.bins[0], bestDist = Infinity;
      s.bins.forEach((b) => {
        const d = Math.abs(b.distance - hoverDistance);
        if (d < bestDist) { bestDist = d; nearest = b; }
      });
      const color = withBins.length === 1 ? getCssVar('--accent') : s.color;
      return `<div class="tt-row"><span class="tt-dot" style="background:${color}"></span>` +
        `<span class="tt-value">${s.column}: ${nearest.mean.toFixed(decimals)} ± ${nearest.std.toFixed(decimals)}` +
        ` · median ${nearest.median.toFixed(decimals)} [${nearest.p25.toFixed(decimals)}–${nearest.p75.toFixed(decimals)}] (n=${nearest.n})</span></div>`;
    }).join('');
    let b0 = withBins[0].bins[0], best = Infinity;
    withBins[0].bins.forEach((b) => { const d = Math.abs(b.distance - hoverDistance); if (d < best) { best = d; b0 = b; } });
    tooltip.innerHTML = `<div class="tt-row"><span class="tt-value">${b0.from.toFixed(0)}–${b0.to.toFixed(0)} m from launch</span></div>` + rows;
    tooltip.style.left = (rect.width / 2) + 'px';
    tooltip.style.top = '0px';
    tooltip.style.display = 'block';
  });

  canvas.addEventListener('mouseleave', () => {
    hoverDistance = null;
    tooltip.style.display = 'none';
    draw();
  });

  return {
    // data: {series: [{column, color, bins}], xLabel, decimals, xMax}
    setData(data) {
      series = data.series || [];
      xLabel = data.xLabel || 'distance (m)';
      decimals = data.decimals ?? 1;
      xMax = data.xMax || 0;
      hoverDistance = null;
      draw();
    },
    setEmptyMessage(message) {
      series = [];
      emptyMessage = message;
      draw();
    },
    getSeries() { return series; },
    draw,
  };
}


// ---------- Downloads: a chart as PNG, its numbers as CSV ----------
// The canvases are drawn at device-pixel resolution with a transparent
// background, so the PNG is composed on an offscreen canvas: panel
// background, a caption line, then the chart as rendered.
function exportCanvasPng(canvas, filename, caption) {
  const dpr = window.devicePixelRatio || 1;
  const out = document.createElement('canvas');
  const ctx = out.getContext('2d');
  const font = `${Math.round(12 * dpr)}px ${getCssVar('--sans') || 'sans-serif'}`;
  ctx.font = font;
  const lines = caption ? wrapTextLines(ctx, caption, canvas.width - 20 * dpr) : [];
  const lineH = Math.round(17 * dpr);
  const captionH = lines.length ? lines.length * lineH + Math.round(10 * dpr) : 0;
  out.width = canvas.width;
  out.height = canvas.height + captionH;
  ctx.fillStyle = getCssVar('--panel') || '#ffffff';
  ctx.fillRect(0, 0, out.width, out.height);
  if (lines.length) {
    ctx.fillStyle = getCssVar('--text') || '#000';
    ctx.font = font;
    ctx.textBaseline = 'middle';
    lines.forEach((line, i) => ctx.fillText(line, Math.round(10 * dpr), Math.round(5 * dpr) + lineH * i + lineH / 2));
  }
  ctx.drawImage(canvas, 0, captionH);
  out.toBlob((blob) => {
    if (!blob) return;
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }, 'image/png');
}

function downloadTextFile(filename, text, mime) {
  const blob = new Blob([text], { type: mime || 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function csvCell(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

function csvFromRows(header, rows) {
  return [header.map(csvCell).join(','), ...rows.map((r) => r.map(csvCell).join(','))].join('\n') + '\n';
}

function safeFilename(s) {
  return String(s).replace(/[^A-Za-z0-9._-]+/g, '_').replace(/^_+|_+$/g, '');
}
