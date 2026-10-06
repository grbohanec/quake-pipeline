"""Build the gold layer: small, purpose-built tables for the dashboard.

    raw (what USGS sent) -> clean (what we trust) -> gold (what a reader needs)

Each gold table answers one question the dashboard asks ("how many quakes per
day?", "what were the strongest this year?"), so the page loads a few kilobytes
instead of 1.5 million rows. Every table is written twice:

* data/gold/<name>.parquet -- synced to S3 with the other layers, queryable in Athena
* <site-dir>/<name>.json   -- the same rows in a compact form the web page reads

Only built after the quality checks pass, so the dashboard never shows a bad build.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

MAP_MIN_MAG = 2.5  # below this, coverage is mostly the US network; the world map would be misleading
MAP_DAYS = 30
DAILY_DAYS = 90
STRONGEST_N = 10
RUNS_SHOWN = 30
JAPAN_BBOX = {"lat": (24, 46), "lon": (122, 150)}


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[list[str], list[list]]:
    cur = con.execute(sql)
    return [d[0] for d in cur.description], [list(r) for r in cur.fetchall()]


def _ts(dt: datetime) -> str:
    return f"TIMESTAMPTZ '{dt.isoformat()}'"


def build_tables(data_dir: Path, now: datetime) -> dict[str, dict]:
    """Return every gold table as {name: {"columns": [...], "rows": [...], ...extra}}."""
    clean_glob = (data_dir / "clean" / "usgs" / "**" / "*.parquet").as_posix()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"CREATE VIEW q AS SELECT * FROM read_parquet('{clean_glob}', hive_partitioning = true)")

    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)
    # Columns the page needs for a single quake. Times as epoch milliseconds: unambiguous in JSON.
    quake_cols = "epoch_ms(event_time) AS time, round(latitude, 3) AS lat, round(longitude, 3) AS lon, "
    quake_cols += "round(depth_km, 1) AS depth, mag, place, event_id AS id"

    tables: dict[str, dict] = {}

    # Headline numbers.
    total, since, last_24h, last_7d = con.execute(f"""
        SELECT count(*), min(year),
               count(*) FILTER (WHERE event_time >= {_ts(now - timedelta(hours=24))}),
               count(*) FILTER (WHERE event_time >= {_ts(now - timedelta(days=7))})
        FROM q
    """).fetchone()
    cols, top = _rows(
        con,
        f"SELECT {quake_cols} FROM q WHERE event_time >= {_ts(month_start)} "
        "ORDER BY mag DESC, event_time LIMIT 1",
    )
    tables["summary"] = {
        "columns": ["archive_total", "archive_since", "last_24h", "last_7d"],
        "rows": [[total, since, last_24h, last_7d]],
        # Flat copies for the page, which reads these fields directly.
        "archive_total": total,
        "archive_since": since,
        "last_24h": last_24h,
        "last_7d": last_7d,
        "largest_this_month": dict(zip(cols, top[0], strict=True)) if top else None,
    }

    # World map: every M2.5+ quake in the last 30 days.
    cols, rows = _rows(
        con,
        f"""
        SELECT {quake_cols} FROM q
        WHERE event_time >= {_ts(now - timedelta(days=MAP_DAYS))} AND mag >= {MAP_MIN_MAG}
        ORDER BY event_time
    """,
    )
    tables["recent_quakes"] = {"columns": cols, "rows": rows}

    # Quakes per day over the last 90 complete UTC days. Days with no quakes still
    # appear (count 0), so a feed outage shows as a dip instead of a silently missing day.
    first_day = today - timedelta(days=DAILY_DAYS)
    cols, rows = _rows(
        con,
        f"""
        WITH days AS (
            SELECT CAST(d AS DATE) AS day
            FROM range({_ts(first_day)}, {_ts(today)}, INTERVAL 1 DAY) AS t(d)
        )
        SELECT strftime(days.day, '%Y-%m-%d') AS day, count(q.event_id) AS quakes
        FROM days LEFT JOIN q ON CAST(q.event_time AS DATE) = days.day
        GROUP BY days.day ORDER BY days.day
    """,
    )
    tables["daily_counts"] = {"columns": cols, "rows": rows}

    # Strongest this year.
    cols, rows = _rows(
        con,
        f"""
        SELECT {quake_cols} FROM q WHERE event_time >= {_ts(year_start)}
        ORDER BY mag DESC, event_time LIMIT {STRONGEST_N}
    """,
    )
    tables["strongest_this_year"] = {"columns": cols, "rows": rows, "year": year_start.year}

    # Japan (the dashboard shows it once the JMA source is added).
    (lat0, lat1), (lon0, lon1) = JAPAN_BBOX["lat"], JAPAN_BBOX["lon"]
    cols, rows = _rows(
        con,
        f"""
        SELECT {quake_cols} FROM q
        WHERE event_time >= {_ts(now - timedelta(days=MAP_DAYS))}
          AND latitude BETWEEN {lat0} AND {lat1} AND longitude BETWEEN {lon0} AND {lon1}
        ORDER BY event_time
    """,
    )
    tables["japan_quakes"] = {"columns": cols, "rows": rows}

    # Pipeline health, from the quality check history (one Parquet file per run).
    dq_files = list((data_dir / "dq" / "results").glob("dq_*.parquet"))
    if dq_files:
        dq_glob = (data_dir / "dq" / "results" / "*.parquet").as_posix()
        con.execute(f"CREATE VIEW dq AS SELECT * FROM read_parquet('{dq_glob}', union_by_name = true)")
        cols, rows = _rows(
            con,
            f"""
            SELECT * FROM (
                SELECT
                    epoch_ms(run_at) AS run_at,
                    bool_and(passed OR severity = 'warn') AS published,
                    count(*) FILTER (WHERE NOT passed AND severity = 'error') AS errors,
                    count(*) FILTER (WHERE NOT passed AND severity = 'warn') AS warnings,
                    max(value) FILTER (WHERE "check" = 'not empty') AS row_count,
                    max(value) FILTER (WHERE "check" = 'data is fresh') AS hours_since_newest_event,
                    coalesce(string_agg("check", ', ') FILTER (WHERE NOT passed), '') AS failed_checks
                FROM dq GROUP BY run_at ORDER BY run_at DESC LIMIT {RUNS_SHOWN}
            ) ORDER BY run_at
        """,
        )
        tables["pipeline_runs"] = {"columns": cols, "rows": rows}
        cols, rows = _rows(
            con,
            """
            SELECT "check", severity, passed, observed, expected FROM dq
            WHERE run_at = (SELECT max(run_at) FROM dq)
        """,
        )
        latest = con.execute("SELECT epoch_ms(max(run_at)) FROM dq").fetchone()[0]
        tables["latest_checks"] = {
            "columns": cols,
            "rows": rows,
            "run_at": latest,
            "checks": [dict(zip(cols, r, strict=True)) for r in rows],
        }
    con.close()
    return tables


def write(tables: dict[str, dict], data_dir: Path, site_dir: Path | None, now: datetime) -> None:
    gold_dir = data_dir / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    if site_dir is not None:
        site_dir.mkdir(parents=True, exist_ok=True)
    for name, t in tables.items():
        cols = t["columns"]
        pq.write_table(
            pa.table({c: [r[i] for r in t["rows"]] for i, c in enumerate(cols)}), gold_dir / f"{name}.parquet"
        )
        if site_dir is not None:
            payload = {"generated_at": now.isoformat(timespec="seconds"), **t}
            (site_dir / f"{name}.json").write_text(json.dumps(payload, separators=(",", ":"), default=str))
    log.info("Gold tables: %s", ", ".join(f"{n} ({len(t['rows'])} rows)" for n, t in tables.items()))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Build the dashboard's gold tables from the clean layer.")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    p.add_argument("--site-dir", type=Path, default=None, help="also write JSON for the web page here")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    now = datetime.now(timezone.utc)
    write(build_tables(args.data_dir, now), args.data_dir, args.site_dir, now)


if __name__ == "__main__":
    main()
