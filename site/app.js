// Dashboard: reads the gold-layer JSON files the daily pipeline publishes in data/.
"use strict";

const PACIFIC_SPLIT = -30; // longitudes west of this are drawn east of 180, so the map centres on the Pacific

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const shiftLon = (lon) => (lon < PACIFIC_SPLIT ? lon + 360 : lon);
const HOME_LON = 152; // map centre: the western Pacific
const COPIES = [-360, 0, 360]; // the world is drawn three times side by side, so panning never runs out of map

// Radius in px from magnitude. Each magnitude step is ~32x more energy, so size
// grows steadily with magnitude rather than with energy, which would hide small quakes.
const radius = (mag, scale = 1) => Math.max(1.5, 1.6 * (mag - 1.6) * scale);

const fmtUTC = new Intl.DateTimeFormat("en-US", {
  timeZone: "UTC", year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hourCycle: "h23",
});
const fmtJST = new Intl.DateTimeFormat("en-US", {
  timeZone: "Asia/Tokyo", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hourCycle: "h23",
});

async function getJSON(path) {
  const r = await fetch(path, { cache: "no-cache" });
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`);
  return r.json();
}

// Shift a whole feature at once (by its mean longitude) so lines and coastlines stay unbroken.
function shiftFeature(f) {
  const g = f.geometry;
  const points = g.type === "LineString" ? g.coordinates : g.coordinates.flat();
  const mean = points.reduce((s, p) => s + p[0], 0) / points.length;
  if (mean >= PACIFIC_SPLIT) return f;
  return offsetFeature(f, 360);
}

function offsetFeature(f, dx) {
  if (!dx) return f;
  const g = f.geometry;
  const mv = (p) => [p[0] + dx, p[1]];
  const coordinates = g.type === "LineString" ? g.coordinates.map(mv) : g.coordinates.map((ring) => ring.map(mv));
  return { ...f, geometry: { ...g, coordinates } };
}

const tiled = (features) => COPIES.flatMap((dx) => features.map((f) => offsetFeature(f, dx)));

function tooltipNode(q) {
  // Built with textContent: place names come from an external API.
  const el = document.createElement("div");
  el.className = "tip";
  const head = document.createElement("strong");
  head.textContent = `M${q.mag.toFixed(1)}`;
  const place = document.createElement("div");
  place.textContent = q.place || "Unknown location";
  const when = document.createElement("div");
  when.className = "muted";
  const t = new Date(q.time);
  when.textContent = `${fmtUTC.format(t)} UTC · ${fmtJST.format(t)} JST`;
  const depth = document.createElement("div");
  depth.className = "muted";
  depth.textContent = `Depth ${Math.round(q.depth)} km`;
  el.append(head, place, when, depth);
  return el;
}

function rowsToObjects({ columns, rows }) {
  return rows.map((r) => Object.fromEntries(columns.map((c, i) => [c, r[i]])));
}

function drawSizeLegend() {
  const box = document.getElementById("size-legend");
  for (const m of [3, 5, 7]) {
    const d = document.createElement("span");
    const px = 2 * radius(m);
    d.className = "dot";
    d.style.width = d.style.height = `${px}px`;
    const label = document.createElement("span");
    label.textContent = `M${m}`;
    box.append(d, label);
  }
  box.style.gap = "6px";
}

function buildMap(land, plates, quakes) {
  const map = L.map("world-map", {
    center: [15, 150],
    zoom: 2,
    minZoom: 0,
    maxZoom: 7,
    zoomSnap: 0,
    preferCanvas: true,
    // Draw a full screen-width beyond each edge, so fast drags never reveal blank map
    // before the next redraw (Leaflet's default is 10%).
    renderer: L.canvas({ padding: 1 }),
    worldCopyJump: true, // while dragging, Leaflet wraps the view by one world width
    attributionControl: false,
  });
  // Fit the full width of the world (Pacific-centred); let the poles crop top and bottom.
  const fitWidth = () => {
    const w = map.getSize().x;
    const zoom = Math.log2((w * 360) / 352 / 256);
    map.options.minZoom = zoom; // set directly: setMinZoom() would start a zoom animation
    map.setView([18, HOME_LON], zoom, { animate: false });
  };
  fitWidth();
  window.addEventListener("resize", fitWidth);

  const landLayer = L.geoJSON(tiled(land.features.map(shiftFeature)), { interactive: false }).addTo(map);
  const plateLayer = L.geoJSON(tiled(plates.features.map(shiftFeature)), { interactive: false }).addTo(map);

  // Biggest first, so small quakes are drawn on top and stay hoverable.
  const sorted = [...quakes].sort((a, b) => b.mag - a.mag);
  const quakeLayer = L.layerGroup(
    COPIES.flatMap((dx) =>
      sorted.map((q) =>
        L.circleMarker([q.lat, shiftLon(q.lon) + dx], { radius: radius(q.mag), weight: 1, mag: q.mag })
          .bindTooltip(() => tooltipNode(q), { direction: "top", offset: [0, -4] }),
      ),
    ),
  ).addTo(map);

  // Endless panning: worldCopyJump keeps the view within one world width while
  // dragging, and the three identical copies make the wrap invisible. This handler
  // covers the other ways to move (zoom, keyboard) and stops scrolling past the poles.
  let adjusting = false;
  map.on("moveend", () => {
    if (adjusting) return;
    const c = map.getCenter();
    const off = c.lng - HOME_LON;
    const lng = Math.abs(off) > 180 ? c.lng - 360 * Math.round(off / 360) : c.lng;
    // Clamp in pixels: the view's top edge may not go above 84°N, nor its bottom below 80°S.
    const half = map.getSize().y / 2;
    const top = map.project([84, 0]).y + half;
    const bottom = map.project([-80, 0]).y - half;
    const y = map.project(c).y;
    const yClamped = top > bottom ? (top + bottom) / 2 : Math.min(Math.max(y, top), bottom);
    if (lng === c.lng && Math.abs(yClamped - y) < 1) return;
    adjusting = true;
    map.setView([map.unproject([0, yClamped]).lat, lng], map.getZoom(), { animate: false });
    adjusting = false;
  });

  // Colours come from CSS tokens, so they follow light/dark mode.
  function applyTheme() {
    landLayer.setStyle({ color: css("--land"), weight: 0, fillColor: css("--land"), fillOpacity: 1 });
    plateLayer.setStyle((f) => {
      const t = f.properties.type;
      const color = css(t === "subduction" || t === "convergent" ? "--plate-convergent" : `--plate-${t}`);
      return { color, weight: t === "subduction" ? 3.5 : 1.6, opacity: 0.95, lineCap: "round", lineJoin: "round" };
    });
    quakeLayer.eachLayer((m) =>
      m.setStyle({ color: css("--surface"), fillColor: css("--quake"), fillOpacity: 0.42, opacity: 0.7 }),
    );
  }
  // Smaller dots on narrow screens, so dense regions don't turn into one blob.
  const rescale = () => {
    const scale = Math.min(1, Math.max(0.45, map.getSize().x / 1100)) * 2 ** ((map.getZoom() - map.getMinZoom()) / 3);
    quakeLayer.eachLayer((m) => m.setRadius(radius(m.options.mag, scale)));
  };
  map.on("zoomend resize", rescale);
  rescale();
  applyTheme();
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", applyTheme);
  return map;
}

const fmtCount = (n) =>
  n >= 1e6 ? `${(n / 1e6).toFixed(2)}M` : n >= 1e4 ? `${(n / 1e3).toFixed(1)}K` : n.toLocaleString("en-US");
const fmtDay = new Intl.DateTimeFormat("en-US", { timeZone: "Asia/Tokyo", month: "short", day: "numeric" });

function setText(id, text) {
  document.getElementById(id).textContent = text;
}

function drawTiles(s) {
  setText("kpi-24h", s.last_24h.toLocaleString("en-US"));
  setText("kpi-7d", s.last_7d.toLocaleString("en-US"));
  if (s.largest_this_month) {
    const q = s.largest_this_month;
    setText("kpi-max", `M${q.mag.toFixed(1)}`);
    setText("kpi-max-note", `${q.place || "Unknown location"} · ${fmtDay.format(new Date(q.time))}`);
    document.getElementById("kpi-max-note").title = q.place || "";
  }
  setText("kpi-total", fmtCount(s.archive_total));
  setText("kpi-total-note", `earthquakes since ${s.archive_since}`);
}

const SVG_NS = "http://www.w3.org/2000/svg";
const svgEl = (tag, attrs = {}, parent) => {
  const el = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  if (parent) parent.append(el);
  return el;
};
const fmtShortDay = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" });
const fmtLongDay = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric", year: "numeric" });
const fmtMonth = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short" });

// Round tick step: 1, 2 or 5 times a power of ten.
function niceStep(max, target = 4) {
  const raw = max / target;
  const pow = 10 ** Math.floor(Math.log10(raw));
  return [1, 2, 5, 10].map((m) => m * pow).find((s) => s >= raw);
}

function drawDailyChart(days) {
  const box = document.getElementById("daily-chart");
  const data = days.map(([day, n]) => ({ date: new Date(`${day}T00:00:00Z`), n }));

  // Table view: every value is reachable without hovering.
  const tbody = document.querySelector("#daily-table tbody");
  tbody.replaceChildren(
    ...[...data].reverse().map((d) => {
      const tr = document.createElement("tr");
      const a = document.createElement("td");
      a.textContent = fmtLongDay.format(d.date);
      const b = document.createElement("td");
      b.className = "num";
      b.textContent = d.n.toLocaleString("en-US");
      tr.append(a, b);
      return tr;
    }),
  );

  function render() {
    box.replaceChildren();
    const W = box.clientWidth, H = box.clientHeight;
    const m = { top: 22, right: 12, bottom: 24, left: 44 };
    const w = W - m.left - m.right, h = H - m.top - m.bottom;
    const maxN = Math.max(...data.map((d) => d.n));
    const step = niceStep(maxN);
    const yMax = Math.ceil(maxN / step) * step;
    const x = (i) => m.left + (data.length === 1 ? w / 2 : (i / (data.length - 1)) * w);
    const y = (n) => m.top + h - (n / yMax) * h;

    const svg = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Line chart of earthquakes per day" }, box);
    const grid = svgEl("g", { class: "grid" }, svg);
    const axis = svgEl("g", { class: "axis" }, svg);
    for (let v = step; v <= yMax; v += step) {
      svgEl("line", { x1: m.left, x2: m.left + w, y1: y(v), y2: y(v) }, grid);
    }
    for (let v = 0; v <= yMax; v += step) {
      const t = svgEl("text", { x: m.left - 8, y: y(v) + 4, "text-anchor": "end" }, axis);
      t.textContent = v.toLocaleString("en-US");
    }
    data.forEach((d, i) => {
      if (d.date.getUTCDate() !== 1) return; // label the first day of each month
      const t = svgEl("text", { x: x(i), y: H - 4, "text-anchor": "middle" }, axis);
      t.textContent = fmtMonth.format(d.date);
    });
    svgEl("line", { class: "baseline", x1: m.left, x2: m.left + w, y1: y(0), y2: y(0) }, svg);

    const pts = data.map((d, i) => `${x(i).toFixed(1)},${y(d.n).toFixed(1)}`);
    svgEl("path", { class: "area", d: `M${x(0)},${y(0)}L${pts.join("L")}L${x(data.length - 1)},${y(0)}Z` }, svg);
    svgEl("path", { class: "line", d: `M${pts.join("L")}` }, svg);

    // Label only the busiest day: usually an aftershock sequence worth noticing.
    const pi = data.reduce((best, d, i) => (d.n > data[best].n ? i : best), 0);
    svgEl("circle", { class: "peak-dot", cx: x(pi), cy: y(data[pi].n), r: 4 }, svg);
    const pl = svgEl("text", { class: "peak-label", x: x(pi), y: y(data[pi].n) - 10, "text-anchor": x(pi) > m.left + w - 60 ? "end" : x(pi) < m.left + 60 ? "start" : "middle" }, svg);
    pl.textContent = `${data[pi].n.toLocaleString("en-US")} on ${fmtShortDay.format(data[pi].date)}`;

    // Crosshair + tooltip: snaps to the nearest day.
    const cross = svgEl("line", { class: "cross", y1: m.top, y2: m.top + h, visibility: "hidden" }, svg);
    const dot = svgEl("circle", { class: "hover-dot", r: 4, visibility: "hidden" }, svg);
    const tip = document.createElement("div");
    tip.className = "chart-tip";
    tip.hidden = true;
    const tipVal = document.createElement("strong");
    const tipDay = document.createElement("span");
    tip.append(tipVal, tipDay);
    box.append(tip);
    const hit = svgEl("rect", { x: m.left, y: 0, width: w, height: H, fill: "transparent" }, svg);
    hit.addEventListener("pointermove", (e) => {
      const r = svg.getBoundingClientRect();
      const px = ((e.clientX - r.left) / r.width) * W;
      const i = Math.max(0, Math.min(data.length - 1, Math.round(((px - m.left) / w) * (data.length - 1))));
      const d = data[i];
      cross.setAttribute("x1", x(i));
      cross.setAttribute("x2", x(i));
      dot.setAttribute("cx", x(i));
      dot.setAttribute("cy", y(d.n));
      cross.setAttribute("visibility", "visible");
      dot.setAttribute("visibility", "visible");
      tipVal.textContent = `${d.n.toLocaleString("en-US")} earthquakes`;
      tipDay.textContent = fmtLongDay.format(d.date);
      tip.hidden = false;
      tip.style.left = `${Math.max(80, Math.min(W - 80, x(i)))}px`;
      tip.style.top = `${y(d.n)}px`;
    });
    hit.addEventListener("pointerleave", () => {
      cross.setAttribute("visibility", "hidden");
      dot.setAttribute("visibility", "hidden");
      tip.hidden = true;
    });
  }
  render();
  new ResizeObserver(render).observe(box);
}

function drawStrongest(table, year) {
  setText("strong-year", String(year));
  const fmtWhen = new Intl.DateTimeFormat("en-US", { timeZone: "Asia/Tokyo", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hourCycle: "h23" });
  const tbody = document.querySelector("#strong-table tbody");
  tbody.replaceChildren(
    ...rowsToObjects(table).map((q) => {
      const tr = document.createElement("tr");
      const cells = [
        ["num mag", q.mag.toFixed(1)],
        ["", q.place || "Unknown location"],
        ["when", fmtWhen.format(new Date(q.time))],
        ["num depth", `${Math.round(q.depth)} km`],
      ];
      for (const [cls, text] of cells) {
        const td = document.createElement("td");
        td.className = cls;
        td.textContent = text;
        tr.append(td);
      }
      return tr;
    }),
  );
}

// Depth bands for the Japan map: shallow crustal / interface / intermediate slab / deep slab.
const DEPTH_BANDS = [
  { max: 30, label: "0–30 km", token: "--depth-1" },
  { max: 70, label: "30–70 km", token: "--depth-2" },
  { max: 300, label: "70–300 km", token: "--depth-3" },
  { max: Infinity, label: "300+ km", token: "--depth-4" },
];
const depthBand = (d) => DEPTH_BANDS.find((b) => d < b.max);

// Rough label positions for the plates that meet around Japan.
const JAPAN_PLATES = [
  { name: "Pacific\nPlate", at: [36.5, 147.2] },
  { name: "Philippine Sea\nPlate", at: [27.2, 134.6] },
  { name: "Okhotsk\nPlate", at: [46.3, 144.2] },
  { name: "Amur\nPlate", at: [39.5, 132.5] },
];

function buildJapanMap(land, plates, quakes) {
  const map = L.map("japan-map", {
    preferCanvas: true,
    attributionControl: false,
    zoomSnap: 0.25,
    minZoom: 4,
    maxZoom: 9,
    maxBounds: [[18, 112], [52, 160]],
    maxBoundsViscosity: 1,
  });
  map.fitBounds([[24.5, 125], [47.5, 148]]);

  const landLayer = L.geoJSON(land, { interactive: false }).addTo(map);
  const plateLayer = L.geoJSON(plates, { interactive: false }).addTo(map);
  for (const p of JAPAN_PLATES) {
    L.marker(p.at, {
      interactive: false,
      icon: L.divIcon({ className: "plate-label", html: "", iconSize: [140, 32], iconAnchor: [70, 16] }),
    })
      .addTo(map)
      .getElement().textContent = p.name.replace("\n", " ");
  }

  // Shallow last, so the many shallow quakes sit on top of the deeper ones.
  const sorted = [...quakes].sort((a, b) => b.depth - a.depth);
  const markers = sorted.map((q) =>
    L.circleMarker([q.lat, q.lon], { radius: radius(q.mag, 1.15), weight: 1, depth: q.depth })
      .bindTooltip(() => tooltipNode(q), { direction: "top", offset: [0, -4] })
      .addTo(map),
  );

  function applyTheme() {
    landLayer.setStyle({ weight: 0, fillColor: css("--land"), fillOpacity: 1 });
    plateLayer.setStyle((f) => ({
      color: css("--plate-line"),
      weight: f.properties.type === "subduction" ? 2.5 : 1.2,
      opacity: 0.8,
      lineCap: "round",
    }));
    for (const m of markers) {
      m.setStyle({ color: css("--surface"), fillColor: css(depthBand(m.options.depth).token), fillOpacity: 0.85, opacity: 0.9 });
    }
  }
  applyTheme();
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", applyTheme);
}

function drawDepthLegend() {
  const box = document.getElementById("depth-legend");
  const title = document.createElement("span");
  title.textContent = "Depth";
  box.append(title);
  for (const b of DEPTH_BANDS) {
    const item = document.createElement("span");
    item.className = "item";
    const sw = document.createElement("span");
    sw.className = "swatch";
    sw.style.background = `var(${b.token})`;
    item.append(sw, document.createTextNode(b.label));
    box.append(item);
  }
  const sep = document.createElement("span");
  sep.className = "item";
  const line = document.createElement("span");
  line.className = "line thick";
  line.style.borderColor = "var(--plate-line)";
  sep.append(line, document.createTextNode("Subduction zone"));
  box.append(sep);
}

function drawJapanSide(quakes) {
  setText("jp-count", quakes.length.toLocaleString("en-US"));
  if (quakes.length) {
    const top = quakes.reduce((a, b) => (b.mag > a.mag ? b : a));
    setText("jp-max", `M${top.mag.toFixed(1)}`);
    setText("jp-max-note", top.place || "");
  }
  const fmtWhen = new Intl.DateTimeFormat("en-US", { timeZone: "Asia/Tokyo", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hourCycle: "h23" });
  const recent = quakes.filter((q) => q.mag >= 4).sort((a, b) => b.time - a.time).slice(0, 10);
  const tbody = document.querySelector("#japan-table tbody");
  tbody.replaceChildren(
    ...recent.map((q) => {
      const tr = document.createElement("tr");
      for (const [cls, text] of [
        ["when", fmtWhen.format(new Date(q.time))],
        ["num mag", q.mag.toFixed(1)],
        ["", q.place || "Unknown location"],
        ["num depth", `${Math.round(q.depth)} km`],
      ]) {
        const td = document.createElement("td");
        td.className = cls;
        td.textContent = text;
        tr.append(td);
      }
      return tr;
    }),
  );
}

async function main() {
  const [land, plates, recent, summary] = await Promise.all([
    getJSON("assets/land.geojson"),
    getJSON("assets/plate_boundaries.geojson"),
    getJSON("data/recent_quakes.json"),
    getJSON("data/summary.json"),
  ]);
  drawTiles(summary);
  const [daily, strongest] = await Promise.all([getJSON("data/daily_counts.json"), getJSON("data/strongest_this_year.json")]);
  drawDailyChart(daily.rows);
  drawStrongest(strongest, strongest.year);
  const japan = rowsToObjects(await getJSON("data/japan_quakes.json"));
  drawDepthLegend();
  drawJapanSide(japan);
  buildJapanMap(land, plates, japan);
  const quakes = rowsToObjects(recent);

  const updated = document.getElementById("updated");
  const gen = new Date(recent.generated_at);
  updated.textContent = `Updated ${fmtJST.format(gen)} JST`;
  if (recent.sample) {
    const flag = document.createElement("span");
    flag.className = "sample-flag";
    flag.textContent = "Sample data";
    updated.append(flag);
  }
  document.getElementById("map-sub").textContent =
    `${quakes.length.toLocaleString("en-US")} earthquakes of M2.5 and larger, on the boundaries between tectonic plates.`;

  drawSizeLegend();
  buildMap(land, plates, quakes);
}

main().catch((err) => {
  document.getElementById("updated").textContent = "Could not load data";
  console.error(err);
});
