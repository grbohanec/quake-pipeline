// Dashboard: reads the gold-layer JSON files the daily pipeline publishes in data/.
"use strict";

const PACIFIC_SPLIT = -30; // longitudes west of this are drawn east of 180, so the map centres on the Pacific

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const shiftLon = (lon) => (lon < PACIFIC_SPLIT ? lon + 360 : lon);

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
  const mv = (p) => [p[0] + 360, p[1]];
  const coordinates = g.type === "LineString" ? g.coordinates.map(mv) : g.coordinates.map((ring) => ring.map(mv));
  return { ...f, geometry: { ...g, coordinates } };
}

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
    worldCopyJump: false,
    maxBounds: [[-85, -60], [85, 360]],
    zoomSnap: 0,
    preferCanvas: true,
    attributionControl: false,
  });
  // Fit the full width of the world (Pacific-centred); let the poles crop top and bottom.
  const fitWidth = () => {
    const w = map.getSize().x;
    const zoom = Math.log2((w * 360) / 352 / 256);
    map.setMinZoom(zoom);
    map.setView([18, 152], zoom, { animate: false });
  };
  fitWidth();
  window.addEventListener("resize", fitWidth);

  const landLayer = L.geoJSON(land.features.map(shiftFeature), { interactive: false }).addTo(map);
  const plateLayer = L.geoJSON(plates.features.map(shiftFeature), { interactive: false }).addTo(map);

  // Biggest first, so small quakes are drawn on top and stay hoverable.
  const sorted = [...quakes].sort((a, b) => b.mag - a.mag);
  const quakeLayer = L.layerGroup(
    sorted.map((q) =>
      L.circleMarker([q.lat, shiftLon(q.lon)], { radius: radius(q.mag), weight: 1, mag: q.mag })
        .bindTooltip(() => tooltipNode(q), { direction: "top", offset: [0, -4] }),
    ),
  ).addTo(map);

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

async function main() {
  const [land, plates, recent] = await Promise.all([
    getJSON("assets/land.geojson"),
    getJSON("assets/plate_boundaries.geojson"),
    getJSON("data/recent_quakes.json"),
  ]);
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
