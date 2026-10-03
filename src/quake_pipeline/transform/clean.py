"""Build the cleaned layer from the raw USGS files.

raw (text, every download, duplicates and revisions included)
  -> clean (typed, one row per earthquake, partitioned by year)

Steps:
1. Read every raw Parquet file.
2. Cast text columns to proper types (timestamps, floats, ints); empty text -> null.
3. De-duplicate: USGS revises events after they happen, and overlapping pulls
   download the same event twice. Keep only the newest version of each event id.
4. Drop rows that fail basic sanity checks (no time/location/magnitude, out-of-range
   coordinates) and report how many were dropped for each reason.
5. Write Parquet partitioned by year, so queries for a time range only read
   the files they need. (Year, not month: early decades have so few events that
   monthly folders would mean thousands of tiny files, which slows reads.)

The whole layer is rebuilt on each run and swapped in at the end, so readers
never see a half-written dataset. DuckDB does the heavy lifting: it streams
through the files instead of loading millions of rows into memory at once.
"""

from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

# raw column -> (clean column name, SQL type)
COLUMNS = {
    "id": ("event_id", "VARCHAR"),
    "time": ("event_time", "TIMESTAMPTZ"),
    "updated": ("updated_at", "TIMESTAMPTZ"),
    "latitude": ("latitude", "DOUBLE"),
    "longitude": ("longitude", "DOUBLE"),
    "depth": ("depth_km", "DOUBLE"),
    "mag": ("mag", "DOUBLE"),
    "magType": ("mag_type", "VARCHAR"),
    "nst": ("nst", "INTEGER"),
    "gap": ("gap", "DOUBLE"),
    "dmin": ("dmin", "DOUBLE"),
    "rms": ("rms", "DOUBLE"),
    "net": ("network", "VARCHAR"),
    "place": ("place", "VARCHAR"),
    "type": ("event_type", "VARCHAR"),
    "horizontalError": ("horizontal_error_km", "DOUBLE"),
    "depthError": ("depth_error_km", "DOUBLE"),
    "magError": ("mag_error", "DOUBLE"),
    "magNst": ("mag_nst", "INTEGER"),
    "status": ("review_status", "VARCHAR"),
    "locationSource": ("location_source", "VARCHAR"),
    "magSource": ("mag_source", "VARCHAR"),
}

# Each check: (reason, SQL condition that marks a row as BAD)
CHECKS = [
    ("missing event_id", "event_id IS NULL"),
    ("missing event_time", "event_time IS NULL"),
    ("missing location", "latitude IS NULL OR longitude IS NULL"),
    ("missing magnitude", "mag IS NULL"),
    ("latitude out of range", "latitude NOT BETWEEN -90 AND 90"),
    ("longitude out of range", "longitude NOT BETWEEN -180 AND 180"),
    ("magnitude out of range", "mag NOT BETWEEN -2 AND 10"),
]


def _typed_select(raw_glob: str) -> str:
    casts = ",\n        ".join(
        f"TRY_CAST(NULLIF(TRIM(\"{raw}\"), '') AS {sql_type}) AS {clean}"
        if sql_type != "VARCHAR"
        else f"NULLIF(TRIM(\"{raw}\"), '') AS {clean}"
        for raw, (clean, sql_type) in COLUMNS.items()
    )
    return f"""
    SELECT
        {casts},
        _source AS source,
        _ingested_at AS ingested_at
    FROM read_parquet('{raw_glob}', union_by_name = true)
    """


def build(data_dir: Path) -> dict:
    raw_glob = (data_dir / "raw" / "usgs" / "*" / "*.parquet").as_posix()
    out_dir = data_dir / "clean" / "usgs"
    tmp_dir = data_dir / "clean" / "_usgs_building"
    if not any((data_dir / "raw" / "usgs").glob("*/*.parquet")):
        raise SystemExit(f"No raw files found under {data_dir / 'raw' / 'usgs'} -- run the ingest first")

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET enable_progress_bar = false")  # keep CI logs readable
    con.execute(f"CREATE TEMP VIEW typed AS {_typed_select(raw_glob)}")

    raw_rows = con.execute("SELECT count(*) FROM typed").fetchone()[0]

    # Newest version of each event wins; ties broken by the most recent download.
    con.execute("""
        CREATE TEMP TABLE latest AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY event_id ORDER BY updated_at DESC NULLS LAST, ingested_at DESC
            ) AS rn
            FROM typed
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

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
            SELECT *, year(event_time) AS year
            FROM latest ORDER BY event_time
        ) TO '{tmp_dir.as_posix()}' (FORMAT parquet, PARTITION_BY (year))
    """)
    con.close()

    # Swap the finished build into place.
    if out_dir.exists():
        shutil.rmtree(out_dir)
    tmp_dir.rename(out_dir)

    summary = {
        "raw_rows": raw_rows,
        "duplicates_removed": raw_rows - unique_rows,
        "dropped": dropped,
        "clean_rows": clean_rows,
    }
    log.info("Raw rows:            %d", raw_rows)
    log.info("Duplicates removed:  %d", summary["duplicates_removed"])
    for reason, n in dropped.items():
        log.info("Dropped (%s): %d", reason, n)
    log.info("Clean rows:          %d  -> %s", clean_rows, out_dir)
    return summary


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Build the cleaned USGS layer from raw files.")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    build(args.data_dir)


if __name__ == "__main__":
    main()
