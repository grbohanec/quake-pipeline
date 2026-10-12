"""Link JMA earthquakes to the same earthquakes in the USGS data.

The two agencies describe one earthquake differently: different ids, origin
times a few seconds apart, epicenters tens of kilometres apart, and different
magnitude scales (JMA's Mj vs. USGS's mostly Mw/mb). There is no shared key,
so records are matched on time and distance:

1. Candidates: every JMA/USGS pair around Japan whose origin times are within
   MAX_SECONDS and whose epicenters are within MAX_KM of each other.
2. Score each pair: seconds / MAX_SECONDS + km / MAX_KM (0 = identical).
3. Keep a pair only if each is the other's best candidate, so every record is
   used at most once (an aftershock can't be matched to its mainshock's record).

Output: clean/jma_usgs_matches/matches.parquet, one row per linked pair. It
answers questions neither source can alone, e.g. how much smaller USGS's
coverage of Japan is, or how Mj compares with Mw for the same quakes.
"""

from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

MAX_SECONDS = 16  # origin times from different networks rarely differ by more than a few seconds
MAX_KM = 100  # locations of offshore quakes can differ by tens of km between agencies
MIN_JMA_MAG = 1.5  # USGS lists M2.0+; this leaves room for Mj reading lower than USGS's magnitude
REGION = {"lat": (20, 50), "lon": (120, 155)}  # Japan and its surrounding seas

OUT_NAME = "jma_usgs_matches"


def build(data_dir: Path) -> dict:
    jma_glob = (data_dir / "clean" / "jma" / "**" / "*.parquet").as_posix()
    usgs_glob = (data_dir / "clean" / "usgs" / "**" / "*.parquet").as_posix()
    for name in ("jma", "usgs"):
        if not any((data_dir / "clean" / name).glob("year=*/*.parquet")):
            raise SystemExit(f"No clean {name} layer under {data_dir / 'clean' / name} -- build it first")

    (lat0, lat1), (lon0, lon1) = REGION["lat"], REGION["lon"]
    in_region = f"latitude BETWEEN {lat0} AND {lat1} AND longitude BETWEEN {lon0} AND {lon1}"

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET enable_progress_bar = false")
    con.execute(f"""
        CREATE TEMP TABLE j AS
        SELECT event_id, event_time, latitude, longitude, depth_km, mag, mag_type
        FROM read_parquet('{jma_glob}', hive_partitioning = true)
        WHERE {in_region} AND mag >= {MIN_JMA_MAG} AND event_type = 'earthquake'
    """)
    con.execute(f"""
        CREATE TEMP TABLE u AS
        SELECT event_id, event_time, latitude, longitude, depth_km, mag, mag_type
        FROM read_parquet('{usgs_glob}', hive_partitioning = true)
        WHERE {in_region}
    """)
    # The BETWEEN on time lets DuckDB use a range join instead of comparing every pair.
    con.execute(f"""
        CREATE TEMP TABLE pairs AS
        SELECT * FROM (
            SELECT
                j.event_id AS jma_event_id,
                u.event_id AS usgs_event_id,
                abs(epoch(u.event_time) - epoch(j.event_time)) AS time_diff_s,
                2 * 6371 * asin(sqrt(
                    pow(sin(radians(u.latitude - j.latitude) / 2), 2)
                    + cos(radians(j.latitude)) * cos(radians(u.latitude))
                      * pow(sin(radians(u.longitude - j.longitude) / 2), 2)
                )) AS distance_km,
                j.event_time AS jma_event_time,
                j.mag AS jma_mag, j.mag_type AS jma_mag_type, j.depth_km AS jma_depth_km,
                u.mag AS usgs_mag, u.mag_type AS usgs_mag_type, u.depth_km AS usgs_depth_km
            FROM j JOIN u
              ON u.event_time BETWEEN j.event_time - INTERVAL {MAX_SECONDS} SECOND
                                  AND j.event_time + INTERVAL {MAX_SECONDS} SECOND
        )
        WHERE distance_km <= {MAX_KM}
    """)
    con.execute(f"""
        CREATE TEMP TABLE matches AS
        SELECT * EXCLUDE (rank_j, rank_u) FROM (
            SELECT *,
                row_number() OVER (PARTITION BY jma_event_id ORDER BY score, usgs_event_id) AS rank_j,
                row_number() OVER (PARTITION BY usgs_event_id ORDER BY score, jma_event_id) AS rank_u
            FROM (
                SELECT *, time_diff_s / {MAX_SECONDS} + distance_km / {MAX_KM} AS score FROM pairs
            )
        ) WHERE rank_j = 1 AND rank_u = 1
    """)

    out_dir = data_dir / "clean" / OUT_NAME
    tmp_dir = data_dir / "clean" / f"_{OUT_NAME}_building"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True)
    con.execute(f"""
        COPY (
            SELECT jma_event_id, usgs_event_id, jma_event_time, round(time_diff_s, 2) AS time_diff_s,
                   round(distance_km, 1) AS distance_km, jma_mag, jma_mag_type, usgs_mag, usgs_mag_type,
                   round(usgs_mag - jma_mag, 2) AS mag_diff, jma_depth_km, usgs_depth_km,
                   round(score, 4) AS score
            FROM matches ORDER BY jma_event_time
        ) TO '{(tmp_dir / "matches.parquet").as_posix()}' (FORMAT parquet)
    """)
    if out_dir.exists():
        shutil.rmtree(out_dir)
    tmp_dir.rename(out_dir)

    n_j, n_u, n_pairs, n_matched = con.execute(
        "SELECT (SELECT count(*) FROM j), (SELECT count(*) FROM u), (SELECT count(*) FROM pairs), "
        "(SELECT count(*) FROM matches)"
    ).fetchone()
    con.close()
    summary = {
        "jma_candidates": n_j,
        "usgs_candidates": n_u,
        "pairs_considered": n_pairs,
        "matched": n_matched,
    }
    log.info(
        "Matched %d pairs (JMA M%.1f+ near Japan: %d, USGS near Japan: %d, candidate pairs: %d)",
        n_matched,
        MIN_JMA_MAG,
        n_j,
        n_u,
        n_pairs,
    )
    return summary


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Link JMA earthquakes to the same earthquakes in USGS.")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    build(args.data_dir)


if __name__ == "__main__":
    main()
