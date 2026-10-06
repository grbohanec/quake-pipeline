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

const fmtUTC = new Intl.DateTimeFormat("en-GB", {
  timeZone: "UTC", year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
});
const fmtJST = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Asia/Tokyo", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
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
const fmtDay = new Intl.DateTimeFormat("en-GB", { timeZone: "Asia/Tokyo", month: "short", day: "numeric" });

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

async function main() {
  const [land, plates, recent, summary] = await Promise.all([
    getJSON("assets/land.geojson"),
    getJSON("assets/plate_boundaries.geojson"),
    getJSON("data/recent_quakes.json"),
    getJSON("data/summary.json"),
  ]);
  drawTiles(summary);
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
