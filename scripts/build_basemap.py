"""Build the dashboard's static map layers: land outlines and plate boundaries.

Run once (the outputs are committed under site/assets/); re-run only to change them.

    python scripts/build_basemap.py

Sources:
* Land: Natural Earth 1:50m land polygons (public domain).
* Plate boundaries: PB2002 model, Bird (2003), "An updated digital model of plate
  boundaries", G-cubed 4(3), doi:10.1029/2001GC000252. GeoJSON conversion by Hugo
  Ahlenius / Nordpil, Open Data Commons Attribution License.

PB2002 splits every boundary into short "steps", each with one of seven classes.
They are grouped into four types and consecutive steps of the same type are
joined into one line, which shrinks ~10 MB of steps to a small file.
"""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

LAND_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_land.geojson"
)
STEPS_URL = "https://raw.githubusercontent.com/fraxen/tectonicplates/master/GeoJSON/PB2002_steps.json"

OUT_DIR = Path(__file__).resolve().parent.parent / "site" / "assets"

# PB2002 step class -> boundary type shown on the map
BOUNDARY_TYPE = {
    "SUB": "subduction",  # subduction zone
    "OCB": "convergent",  # oceanic convergent boundary (no subduction)
    "CCB": "convergent",  # continental convergent boundary, e.g. the Himalayas
    "OSR": "divergent",  # oceanic spreading ridge
    "CRB": "divergent",  # continental rift boundary
    "OTF": "transform",  # oceanic transform fault
    "CTF": "transform",  # continental transform fault, e.g. San Andreas
}

DECIMALS = 2  # ~1 km; plenty for a world map, and keeps the files small
MIN_RING_POINTS = 4  # drop slivers that collapse after rounding


def _get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=120) as r:
        return json.load(r)


def _pt(lon: float, lat: float) -> list[float]:
    return [round(lon, DECIMALS), round(lat, DECIMALS)]


def _ring(coords: list) -> list | None:
    out = []
    for lon, lat in coords:
        p = _pt(lon, lat)
        if not out or p != out[-1]:
            out.append(p)
    return out if len(out) >= MIN_RING_POINTS else None


def build_land(src: dict) -> dict:
    features = []
    for f in src["features"]:
        g = f["geometry"]
        polys = g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]]
        for poly in polys:
            rings = [r for r in (_ring(ring) for ring in poly) if r]
            if rings:
                features.append({"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": rings}})
    return {"type": "FeatureCollection", "features": features}


def build_boundaries(src: dict) -> dict:
    steps = sorted(
        (f["properties"] for f in src["features"]),
        key=lambda p: (p["PLATEBOUND"], p["SEQNUM"]),
    )
    features, line, current = [], [], None

    def flush():
        if current and len(line) >= 2:
            features.append(
                {
                    "type": "Feature",
                    "properties": {"type": current[1], "boundary": current[0]},
                    "geometry": {"type": "LineString", "coordinates": line},
                }
            )

    for p in steps:
        key = (p["PLATEBOUND"], BOUNDARY_TYPE[p["STEPCLASS"]])
        start, end = _pt(p["STARTLONG"], p["STARTLAT"]), _pt(p["FINALLONG"], p["FINALLAT"])
        crosses_dateline = abs(end[0] - start[0]) > 180
        if key != current or crosses_dateline or (line and line[-1] != start):
            flush()
            line, current = [start], key
        if crosses_dateline:
            line, current = [], None  # skip the one step that wraps around the map
            continue
        line.append(end)
    flush()
    return {"type": "FeatureCollection", "features": features}


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, url, build in [
        ("land.geojson", LAND_URL, build_land),
        ("plate_boundaries.geojson", STEPS_URL, build_boundaries),
    ]:
        data = build(_get(url))
        path = OUT_DIR / name
        path.write_text(json.dumps(data, separators=(",", ":")))
        print(f"{path.name}: {len(data['features'])} features, {path.stat().st_size / 1e3:.0f} KB")


if __name__ == "__main__":
    main()
