"""Build the clean JMA layers from the raw files.

Two tables come out of this step:

clean/jma/year=YYYY/        every earthquake JMA located, one row each
    catalog rows (final) for the years the catalog covers, then daily-list rows
    (preliminary) for every day after. When JMA publishes the next catalog year,
    its final rows replace that year's preliminary ones on the next rebuild, with
    no extra code: the cutoff moves forward by itself.

clean/jma_felt/year=YYYY/   earthquakes felt in Japan, with shindo (震度, seismic
    intensity: how strongly people felt the shaking, JMA's 0-7 scale), from the
    near-real-time feed. One row per JMA event id, from its latest bulletin.

Parsing notes:
* All JMA times are Japan Standard Time (UTC+9, no daylight saving since 1951).
  The catalog is converted with a fixed +9h offset rather than the Asia/Tokyo
  zone, because the zone database applies Japan's 1948-51 daylight saving time
  while the catalog doesn't.
* The catalog is fixed-width text with implied decimals ("1234" in a F4.2 field
  means 12.34). It is decoded as latin-1, which maps every byte to one character,
  so column positions in the spec (counted in bytes) stay correct.
* Magnitude can be missing for very small quakes. Those rows are kept (they are
  real, located earthquakes); only rows without a time or location are dropped.
* Daily lists only give Japanese region names. Each one gets an English name
  from the catalog: the most common catalog name for quakes in the same
  0.1° x 0.1° cell (about 10 km) since 2000, or the nearest neighbouring cell.

Like the USGS clean step, both tables are rebuilt from raw on each run and
swapped in at the end.
"""

from __future__ import annotations

import argparse
import gzip
import html
import json
import logging
import re
import shutil
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow as pa

from quake_pipeline.transform.clean import _write_json

log = logging.getLogger(__name__)

SUMMARY_FILE = "clean_summary_jma.json"
JST_OFFSET = "INTERVAL 9 HOUR"
UNIT_SEP = chr(31)  # a delimiter that never appears in the text, so each line is read whole

# Clean schema for clean/jma (also used by the quality checks).
COLUMNS = {
    "event_id": "VARCHAR",
    "event_time": "TIMESTAMPTZ",
    "latitude": "DOUBLE",
    "longitude": "DOUBLE",
    "depth_km": "DOUBLE",
    "mag": "DOUBLE",
    "mag_type": "VARCHAR",
    "max_intensity": "VARCHAR",
    "event_type": "VARCHAR",
    "region": "VARCHAR",
    "region_en": "VARCHAR",
    "agency": "VARCHAR",
    "status": "VARCHAR",
    "source": "VARCHAR",
}

FELT_COLUMNS = {
    "report_id": "VARCHAR",
    "event_time": "TIMESTAMPTZ",
    "latitude": "DOUBLE",
    "longitude": "DOUBLE",
    "depth_km": "DOUBLE",
    "mag": "DOUBLE",
    "max_intensity": "VARCHAR",
    "region": "VARCHAR",
    "region_en": "VARCHAR",
    "updated_at": "TIMESTAMPTZ",
}

SHINDO = ["1", "2", "3", "4", "5-", "5+", "6-", "6+", "7", "5", "6"]  # 5 and 6 were single levels until 1996

# Catalog codes -> readable values (from the JMA hypocenter format spec).
CATALOG_INTENSITY = {"1": "1", "2": "2", "3": "3", "4": "4", "5": "5", "6": "6", "7": "7"}
CATALOG_INTENSITY |= {"A": "5-", "B": "5+", "C": "6-", "D": "6+"}
CATALOG_EVENT_TYPE = {"3": "artificial", "4": "volcanic", "5": "low_frequency"}  # others: earthquake
CATALOG_MAG_TYPE = {
    "J": "Mj",
    "D": "Md",
    "d": "Md",
    "V": "Mv",
    "v": "Mv",
    "W": "Mw",
    "B": "mb",
    "S": "Ms",
}

# Each check: (reason, SQL condition that marks a row as BAD). Magnitude may be null.
CHECKS = [
    ("missing event_time", "event_time IS NULL"),
    ("missing location", "latitude IS NULL OR longitude IS NULL"),
    ("latitude out of range", "latitude NOT BETWEEN -90 AND 90"),
    ("longitude out of range", "longitude NOT BETWEEN -180 AND 180"),
    ("magnitude out of range", "mag NOT BETWEEN -3 AND 10"),
]

# A daily-list data line, e.g.
# 2024  1  1 00:00  4.5  32°38.6'N 131°59.1'E   36     0.4  日向灘
DAILY_LINE = re.compile(
    r"^\s*(\d{4})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2}):(\d{2})\s+(\d{1,2}(?:\.\d+)?)\s+"
    r"(\d{1,3})°\s*(\d{1,2}(?:\.\d+)?)'([NS])\s*(\d{1,3})°\s*(\d{1,2}(?:\.\d+)?)'([EW])\s+"
    r"(-|\d+(?:\.\d+)?)\s+(-|-?\d+(?:\.\d+)?)\s+(\S.*?)\s*$"
)
TAG = re.compile(r"<[^>]+>")

# ISO 6709 point used by the feed: "+32.0+131.4-10000/" (depth in metres, negative = below ground).
FEED_COORD = re.compile(r"^([+-]\d+(?:\.\d+)?)([+-]\d+(?:\.\d+)?)([+-]\d+(?:\.\d+)?)?/$")


# --- Catalog (SQL over fixed-width lines) -----------------------------------


def _fw(start: int, width: int, decimals: int = 0) -> str:
    """SQL for a fixed-width numeric field with implied decimals; blank -> NULL.

    Positions are 1-based, as in the JMA spec. The integer part may carry a sign;
    a blank decimal part (used for fixed hypocenters) counts as zero.
    """
    s = f"substr(line, {start}, {width})"
    if decimals == 0:
        return f"TRY_CAST(NULLIF(trim({s}), '') AS DOUBLE)"
    ip = f"substr(line, {start}, {width - decimals})"
    fp = f"substr(line, {start + width - decimals}, {decimals})"
    return (
        f"CASE WHEN trim({s}) = '' THEN NULL ELSE "
        f"(CASE WHEN contains({ip}, '-') THEN -1 ELSE 1 END) * ("
        f"abs(coalesce(TRY_CAST(NULLIF(replace(trim({ip}), '-', ''), '') AS DOUBLE), 0)) "
        f"+ coalesce(TRY_CAST(NULLIF(trim({fp}), '') AS DOUBLE), 0) / {10**decimals}) END"
    )


def _catalog_mag(start: int) -> str:
    """Magnitude field (F2.1): '45' = 4.5, ' 5' = 0.5. Negative values use codes:
    '-1' = -0.1, 'A0' = -1.0, 'B5' = -2.5, 'C0' = -3.0."""
    s = f"substr(line, {start}, 2)"
    first, second = f"substr({s}, 1, 1)", f"substr({s}, 2, 1)"
    return f"""CASE
        WHEN trim({s}) = '' THEN NULL
        WHEN {first} = '-' THEN -TRY_CAST({second} AS DOUBLE) / 10
        WHEN {first} IN ('A', 'B', 'C') THEN
            -((strpos('ABC', {first})) + TRY_CAST({second} AS DOUBLE) / 10)
        ELSE coalesce(TRY_CAST(NULLIF(trim({first}), '') AS DOUBLE), 0) + TRY_CAST({second} AS DOUBLE) / 10
    END"""


def _map(expr: str, mapping: dict[str, str], default: str = "NULL") -> str:
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in mapping.items())
    return f"CASE {expr} {whens} ELSE {default} END"


def catalog_select(lines_glob: str) -> str:
    sec = _fw(14, 4, 2)
    lat = f"(CASE WHEN {_fw(22, 3)} < 0 THEN -1 ELSE 1 END) * (abs({_fw(22, 3)}) + {_fw(25, 4, 2)} / 60)"
    lon = f"(CASE WHEN {_fw(33, 4)} < 0 THEN -1 ELSE 1 END) * (abs({_fw(33, 4)}) + {_fw(37, 4, 2)} / 60)"
    m1, m2 = _catalog_mag(53), _catalog_mag(56)
    t1, t2 = "substr(line, 55, 1)", "substr(line, 58, 1)"
    return f"""
    SELECT
        CAST(TRY(make_timestamp(
            TRY_CAST(substr(line, 2, 4) AS BIGINT), TRY_CAST(substr(line, 6, 2) AS BIGINT),
            TRY_CAST(substr(line, 8, 2) AS BIGINT), TRY_CAST(substr(line, 10, 2) AS BIGINT),
            TRY_CAST(substr(line, 12, 2) AS BIGINT), coalesce({sec}, 0)
        ) - {JST_OFFSET}) AS TIMESTAMPTZ) AS event_time,
        {lat} AS latitude,
        {lon} AS longitude,
        {_fw(45, 5, 2)} AS depth_km,
        coalesce({m1}, {m2}) AS mag,
        {_map(f"CASE WHEN {m1} IS NOT NULL THEN {t1} ELSE {t2} END", CATALOG_MAG_TYPE)} AS mag_type,
        {_map("substr(line, 62, 1)", CATALOG_INTENSITY)} AS max_intensity,
        {_map("substr(line, 61, 1)", CATALOG_EVENT_TYPE, "'earthquake'")} AS event_type,
        NULLIF(trim(substr(line, 69, 24)), '') AS region,
        substr(line, 1, 1) AS agency,
        'final' AS status,
        'jma_catalog' AS source
    FROM read_csv('{lines_glob}', columns = {{'line': 'VARCHAR'}}, delim = '{UNIT_SEP}', quote = '',
                  escape = '', header = false, auto_detect = false)
    WHERE length(line) >= 68
    """


def unpack_catalog(raw_dir: Path, out_dir: Path) -> int:
    """Write every catalog zip member as UTF-8 text (via latin-1, so byte columns are preserved)."""
    n = 0
    for zpath in sorted((raw_dir / "catalog").glob("h*.zip")):
        with zipfile.ZipFile(zpath) as z:
            for member in z.infolist():
                if member.is_dir():
                    continue
                text = z.read(member).decode("latin-1")
                (out_dir / f"{zpath.stem}_{n:03d}.txt").write_text(text, encoding="utf-8")
                n += 1
    return n


# --- Daily lists (Python, then into DuckDB) ---------------------------------


def _decode(page: bytes) -> str:
    head = page[:2000].decode("ascii", "ignore").lower()
    m = re.search(r"charset=[\"']?([\w-]+)", head)
    for enc in [m.group(1)] if m else []:
        try:
            return page.decode(enc)
        except (LookupError, UnicodeDecodeError):
            pass
    try:
        return page.decode("utf-8")
    except UnicodeDecodeError:
        return page.decode("cp932", "replace")


def parse_daily(page: bytes) -> list[dict]:
    """Every hypocenter line on a daily-list page, as a dict of typed values (times in UTC)."""
    text = html.unescape(TAG.sub("\n", _decode(page)))  # a tag can end a line
    rows = []
    for line in text.splitlines():
        m = DAILY_LINE.match(line.replace("　", " "))
        if not m:
            continue
        y, mo, d, h, mi, s, lad, lam, ns, lod, lom, ew, depth, mag, region = m.groups()
        jst = datetime(int(y), int(mo), int(d), int(h), int(mi)) + timedelta(seconds=float(s))
        lat = (int(lad) + float(lam) / 60) * (1 if ns == "N" else -1)
        lon = (int(lod) + float(lom) / 60) * (1 if ew == "E" else -1)
        rows.append(
            {
                "event_time": (jst - timedelta(hours=9)).replace(tzinfo=timezone.utc),
                "latitude": lat,
                "longitude": lon,
                "depth_km": None if depth == "-" else float(depth),
                "mag": None if mag == "-" else float(mag),
                "region": region,
            }
        )
    return rows


def daily_table(raw_dir: Path) -> pa.Table:
    rows = []
    for path in sorted((raw_dir / "daily").glob("*/*.html.gz")):
        rows += [r | {"day": path.name[:8]} for r in parse_daily(gzip.decompress(path.read_bytes()))]
    schema = pa.schema(
        [
            ("event_time", pa.timestamp("us", tz="UTC")),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("depth_km", pa.float64()),
            ("mag", pa.float64()),
            ("region", pa.string()),
            ("day", pa.string()),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


# --- Felt-earthquake feed ---------------------------------------------------


def parse_feed(entries: list[dict]) -> list[dict]:
    """Bulletins that carry both a location and a shindo (震源・震度情報), typed, times in UTC."""
    rows = []
    for e in entries:
        m = FEED_COORD.match(e.get("cod") or "")
        shindo = (e.get("maxi") or "").strip()
        if not (m and shindo and e.get("eid") and e.get("at")):
            continue
        try:
            mag = float(e.get("mag"))
        except (TypeError, ValueError):
            mag = None  # e.g. "Ｍ不明" (magnitude unknown)
        depth_m = float(m.group(3)) if m.group(3) else None
        rows.append(
            {
                "report_id": str(e["eid"]),
                "event_time": datetime.fromisoformat(e["at"]).astimezone(timezone.utc),
                "latitude": float(m.group(1)),
                "longitude": float(m.group(2)),
                "depth_km": None if depth_m is None else -depth_m / 1000,
                "mag": mag if mag == mag else None,  # NaN -> None
                "max_intensity": shindo,
                "region": e.get("anm") or None,
                "region_en": e.get("en_anm") or None,
                "updated_at": datetime.fromisoformat(e["rdt"]).astimezone(timezone.utc)
                if e.get("rdt")
                else None,
            }
        )
    return rows


def feed_table(raw_dir: Path) -> pa.Table:
    rows = []
    for path in sorted((raw_dir / "feed").glob("run_id=*/list.json.gz")):
        rows += [
            r | {"snapshot": path.parent.name}
            for r in parse_feed(json.loads(gzip.decompress(path.read_bytes())))
        ]
    ts = pa.timestamp("us", tz="UTC")
    schema = pa.schema(
        [
            ("report_id", pa.string()),
            ("event_time", ts),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("depth_km", pa.float64()),
            ("mag", pa.float64()),
            ("max_intensity", pa.string()),
            ("region", pa.string()),
            ("region_en", pa.string()),
            ("updated_at", ts),
            ("snapshot", pa.string()),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


# --- Build -------------------------------------------------------------------

# Readable and stable for a given tier: origin time to 1/100 s plus a short hash of the location
# (micro-earthquake swarms can have two events in the same second).
EVENT_ID_SQL = """'jma-' || strftime(event_time, '%Y%m%dT%H%M%S') || '.'
    || lpad(CAST((epoch_ms(event_time) % 1000) // 10 AS VARCHAR), 2, '0')
    || '-' || substr(md5(printf('%.2f,%.2f', latitude, longitude)), 1, 4)"""


def _swap(con: duckdb.DuckDBPyConnection, select: str, data_dir: Path, name: str) -> None:
    out_dir, tmp_dir = data_dir / "clean" / name, data_dir / "clean" / f"_{name}_building"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.parent.mkdir(parents=True, exist_ok=True)
    if con.execute(f"SELECT count(*) FROM ({select})").fetchone()[0]:  # no files for an empty table
        con.execute(f"COPY ({select}) TO '{tmp_dir.as_posix()}' (FORMAT parquet, PARTITION_BY (year))")
    if out_dir.exists():
        shutil.rmtree(out_dir)
    if tmp_dir.exists():
        tmp_dir.rename(out_dir)


def build(data_dir: Path) -> dict:
    raw_dir = data_dir / "raw" / "jma"
    has_catalog = any((raw_dir / "catalog").glob("h*.zip"))
    has_daily = any((raw_dir / "daily").glob("*/*.html.gz"))
    if not (has_catalog or has_daily):
        raise SystemExit(f"No raw JMA catalog or daily files under {raw_dir} -- run quake-ingest-jma first")

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET enable_progress_bar = false")

    with tempfile.TemporaryDirectory() as tmp:
        cols = ", ".join(
            f"NULL::{t} AS {c}" for c, t in COLUMNS.items() if c not in ("event_id", "region_en")
        )
        empty = f"SELECT {cols} WHERE false"
        if has_catalog and unpack_catalog(raw_dir, Path(tmp)):
            con.execute(f"CREATE TEMP TABLE catalog AS {catalog_select((Path(tmp) / '*.txt').as_posix())}")
        else:
            con.execute(f"CREATE TEMP TABLE catalog AS {empty}")

    daily = daily_table(raw_dir)  # noqa: F841 -- read by DuckDB below
    # The catalog wins for every year it covers; daily lists fill the days after it.
    cutoff = con.execute(
        f"SELECT CAST(year(max(event_time) + {JST_OFFSET}) + 1 AS VARCHAR) || '0101' FROM catalog"
    ).fetchone()[0]
    cutoff = cutoff or "00000101"
    con.execute(f"""
        CREATE TEMP TABLE preliminary AS
        SELECT event_time, latitude, longitude, depth_km, mag,
               CASE WHEN mag IS NULL THEN NULL ELSE 'Mj' END AS mag_type,
               NULL::VARCHAR AS max_intensity, 'earthquake' AS event_type, region,
               'J' AS agency, 'preliminary' AS status, 'jma_daily' AS source
        FROM daily WHERE day >= '{cutoff}'
    """)

    # English names for daily-list regions: the catalog's most common name in the
    # same ~10 km cell, or else in the nearest neighbouring cell that has one.
    con.execute("""
        CREATE TEMP TABLE region_cells AS
        SELECT ci, cj, arg_max(region, n) AS region_en, max(n) AS n FROM (
            SELECT CAST(floor(latitude * 10) AS INT) AS ci, CAST(floor(longitude * 10) AS INT) AS cj,
                   region, count(*) AS n
            FROM catalog WHERE agency = 'J' AND region IS NOT NULL AND year(event_time) >= 2000
            GROUP BY ALL
        ) GROUP BY ci, cj
    """)
    # Each row looks up its own cell and the 8 around it. Spelled out as 9 equality
    # keys rather than a range condition, so DuckDB can use a fast hash join.
    con.execute("""
        CREATE TEMP TABLE preliminary_named AS
        SELECT * EXCLUDE (rid, rn) FROM (
            SELECT p.*, c.region_en, row_number() OVER (
                PARTITION BY p.rid ORDER BY c.region_en IS NULL, abs(o.di) + abs(o.dj), c.n DESC
            ) AS rn
            FROM (
                SELECT *, row_number() OVER () AS rid,
                       CAST(floor(latitude * 10) AS INT) AS ci, CAST(floor(longitude * 10) AS INT) AS cj
                FROM preliminary
            ) p
            CROSS JOIN (SELECT a AS di, b AS dj FROM range(-1, 2) t(a), range(-1, 2) u(b)) o
            LEFT JOIN region_cells c ON c.ci = p.ci + o.di AND c.cj = p.cj + o.dj
        ) WHERE rn = 1
    """)

    con.execute(f"""
        CREATE TEMP TABLE typed AS
        SELECT {EVENT_ID_SQL} AS event_id, * FROM (
            SELECT *, region AS region_en FROM catalog
            UNION ALL BY NAME
            SELECT * EXCLUDE (ci, cj) FROM preliminary_named
        )
    """)
    raw_rows = con.execute("SELECT count(*) FROM typed").fetchone()[0]
    by_status = dict(con.execute("SELECT status, count(*) FROM typed GROUP BY status").fetchall())

    # The same event can appear twice (overlapping catalog files, re-downloaded days).
    con.execute("""
        CREATE TEMP TABLE latest AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY status) AS rn FROM typed
        ) WHERE rn = 1 OR event_id IS NULL
    """)
    unique_rows = con.execute("SELECT count(*) FROM latest").fetchone()[0]

    dropped = {}
    for reason, bad in CHECKS:
        n = con.execute(f"SELECT count(*) FROM latest WHERE {bad}").fetchone()[0]
        if n:
            dropped[reason] = n
            con.execute(f"DELETE FROM latest WHERE {bad}")
    clean_rows = con.execute("SELECT count(*) FROM latest").fetchone()[0]

    order = ", ".join(COLUMNS)
    _swap(con, f"SELECT {order}, year(event_time) AS year FROM latest ORDER BY event_time", data_dir, "jma")

    # Felt earthquakes: newest bulletin per JMA event id, across all saved snapshots.
    feed = feed_table(raw_dir)  # noqa: F841 -- read by DuckDB below
    felt_cols = ", ".join(FELT_COLUMNS)
    con.execute(f"""
        CREATE TEMP TABLE felt AS
        SELECT {felt_cols} FROM (
            SELECT *, row_number() OVER (
                PARTITION BY report_id ORDER BY updated_at DESC NULLS LAST, snapshot DESC
            ) AS rn FROM feed
        ) WHERE rn = 1
    """)
    felt_rows = con.execute("SELECT count(*) FROM felt").fetchone()[0]
    _swap(con, "SELECT *, year(event_time) AS year FROM felt ORDER BY event_time", data_dir, "jma_felt")
    con.close()

    summary = {
        "raw_rows": raw_rows,
        "rows_by_status": by_status,
        "catalog_covers_until": cutoff,
        "duplicates_removed": raw_rows - unique_rows,
        "dropped": dropped,
        "clean_rows": clean_rows,
        "felt_rows": felt_rows,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _write_json(data_dir / "state" / SUMMARY_FILE, summary)
    log.info("Raw rows:            %d  %s", raw_rows, by_status)
    log.info("Duplicates removed:  %d", summary["duplicates_removed"])
    for reason, n in dropped.items():
        log.info("Dropped (%s): %d", reason, n)
    log.info("Clean rows:          %d  -> %s", clean_rows, data_dir / "clean" / "jma")
    log.info("Felt earthquakes:    %d  -> %s", felt_rows, data_dir / "clean" / "jma_felt")
    return summary


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Build the clean JMA layers from raw files.")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    build(args.data_dir)


if __name__ == "__main__":
    main()
