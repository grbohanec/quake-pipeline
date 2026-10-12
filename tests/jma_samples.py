"""Sample JMA files for tests, built to JMA's published formats.

The ingest and clean tests run offline, so these helpers write raw files the way
JMA serves them: catalog lines in the 96-byte fixed-width layout
(https://www.data.jma.go.jp/eqev/data/bulletin/data/format/hypfmt_e.html), a
daily-list page, and the felt-earthquake feed's JSON.
"""

from __future__ import annotations

import gzip
import io
import json
import zipfile
from datetime import datetime
from pathlib import Path


def _mag_code(mag: float | None) -> str:
    """Magnitude as the catalog's F2.1 field, including its codes for negative values."""
    if mag is None:
        return "  "
    tenths = round(mag * 10)
    if tenths >= 0:
        return f"{tenths:2d}"
    if tenths > -10:
        return f"-{-tenths}"
    return "ABC"[(-tenths) // 10 - 1] + str((-tenths) % 10)


def catalog_line(
    when_jst: datetime,
    lat: float,
    lon: float,
    depth: float = 10.0,
    mag: float | None = 3.0,
    mag_type: str = "J",
    intensity: str = " ",
    subsidiary: str = "1",
    region: str = "NE FUKUOKA PREF",
    agency: str = "J",
) -> str:
    sec = when_jst.second + when_jst.microsecond / 1e6
    lat_deg, lat_min = int(abs(lat)), (abs(lat) % 1) * 60
    lon_deg, lon_min = int(abs(lon)), (abs(lon) % 1) * 60
    lat_deg *= -1 if lat < 0 else 1
    lon_deg *= -1 if lon < 0 else 1
    line = (
        f"{agency}{when_jst:%Y%m%d%H%M}{round(sec * 100):4d}  10"
        f"{lat_deg:3d}{round(lat_min * 100):4d}  20"
        f"{lon_deg:4d}{round(lon_min * 100):4d}  30"
        f"{round(depth * 100):5d} 50"
        f"{_mag_code(mag)}{mag_type if mag is not None else ' '}   51{subsidiary}{intensity}  7261"
        f"{region:<24}"
        f" 25K"
    )
    assert len(line) == 96, len(line)
    return line


def write_catalog_zip(raw_dir: Path, name: str, lines: list[str]) -> Path:
    path = raw_dir / "catalog" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name.replace(".zip", ""), ("\r\n".join(lines) + "\r\n").encode("latin-1"))
    path.write_bytes(buf.getvalue())
    return path


def daily_line(when_jst: datetime, lat: float, lon: float, depth="10", mag="1.2", region="茨城県沖") -> str:
    def dm(v: float) -> str:
        return f"{int(v):>3d}°{(v % 1) * 60:4.1f}'"

    sec = when_jst.second + when_jst.microsecond / 1e6
    return (
        f"{when_jst.year} {when_jst.month:2d} {when_jst.day:2d} {when_jst:%H:%M} {sec:4.1f} "
        f"{dm(lat)}N {dm(lon)}E {depth:>4} {mag:>7}  {region}"
    )


def daily_page(lines: list[str], encoding: str = "utf-8") -> bytes:
    body = "\n".join(["年   月 日 時 分 秒    緯度       経度       深さ(km)  Ｍ   震央地名", *lines])
    page = (
        f'<!DOCTYPE html><html lang="ja"><head><meta charset="{encoding}"><title>震源リスト</title></head>'
        f"<body><h1>震源リスト</h1><pre>{body}</pre><p>&copy; 気象庁</p></body></html>"
    )
    return page.encode(encoding)


def write_daily(raw_dir: Path, day: str, lines: list[str], encoding: str = "utf-8") -> Path:
    path = raw_dir / "daily" / day[:4] / f"{day}.html.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(daily_page(lines, encoding)))
    return path


def feed_entry(
    eid: str,
    at_jst: str,
    cod: str = "+32.0+131.4-10000/",
    mag: str = "2.3",
    maxi: str = "1",
    rdt: str | None = None,
    ttl: str = "震源・震度情報",
) -> dict:
    return {
        "ctt": eid,
        "eid": eid,
        "rdt": rdt or at_jst,
        "ttl": ttl,
        "ift": "発表",
        "ser": "1",
        "at": at_jst,
        "anm": "宮崎県南部平野部",
        "acd": "762",
        "cod": cod,
        "mag": mag,
        "maxi": maxi,
        "int": [],
        "json": f"{eid}_VXSE53.json",
        "en_ttl": "Earthquake and Seismic Intensity Information",
        "en_anm": "Plain Area of Southern Miyazaki Prefecture",
    }


def write_feed(raw_dir: Path, run_id: str, entries: list[dict]) -> Path:
    path = raw_dir / "feed" / f"run_id={run_id}" / "list.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(json.dumps(entries, ensure_ascii=False).encode("utf-8")))
    return path
