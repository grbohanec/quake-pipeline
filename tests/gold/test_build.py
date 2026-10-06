"""Tests for the gold layer."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from quake_pipeline.gold import build as gold
from quake_pipeline.quality import checks
from quake_pipeline.transform import clean

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def quake(id, when, mag="3.0", lat="35.6", lon="139.7", depth="10", place="Somewhere"):
    row = {c: "" for c in clean.COLUMNS}
    row.update(
        id=id,
        time=iso(when),
        updated=iso(when),
        latitude=lat,
        longitude=lon,
        depth=depth,
        mag=mag,
        place=place,
    )
    return row


def build_clean(data_dir: Path, rows) -> None:
    d = data_dir / "raw" / "usgs" / "run_id=x"
    d.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, dtype=str).assign(_source="usgs", _ingested_at="x").to_parquet(
        d / "p.parquet", index=False
    )
    clean.build(data_dir)


@pytest.fixture
def tables(tmp_path):
    rows = [
        quake("old", datetime(1923, 9, 1, 2, 58, tzinfo=timezone.utc), mag="7.9"),
        quake("last_year", datetime(2025, 7, 1, tzinfo=timezone.utc), mag="8.0"),
        quake("this_year_big", datetime(2026, 3, 1, tzinfo=timezone.utc), mag="7.1", place="Big one"),
        quake("month_big", NOW - timedelta(days=3), mag="6.4", place="Month max"),
        quake("small_recent", NOW - timedelta(hours=2), mag="2.2"),  # in counts, not on the map
        quake("week", NOW - timedelta(days=5), mag="4.0"),
        quake("japan", NOW - timedelta(days=10), mag="5.0", lat="38.3", lon="142.4"),
        quake("chile", NOW - timedelta(days=10), mag="5.0", lat="-33.0", lon="-72.0"),
        quake("too_old_for_map", NOW - timedelta(days=40), mag="5.5"),
        quake("today", NOW - timedelta(minutes=30), mag="3.0"),  # today: not a complete day yet
    ]
    build_clean(tmp_path, rows)
    return gold.build_tables(tmp_path, NOW), tmp_path


def objects(t):
    return [dict(zip(t["columns"], r, strict=True)) for r in t["rows"]]


def test_summary(tables):
    t, _ = tables
    s = t["summary"]
    assert s["archive_total"] == 10 and s["archive_since"] == 1923
    assert s["last_24h"] == 2  # small_recent + today
    assert s["last_7d"] == 4  # + month_big, week
    assert s["largest_this_month"]["place"] == "Month max"


def test_map_shows_last_30_days_m25_plus(tables):
    t, _ = tables
    ids = {q["id"] for q in objects(t["recent_quakes"])}
    assert ids == {"month_big", "week", "japan", "chile", "today"}


def test_daily_counts_are_complete_days_including_zeros(tables):
    t, _ = tables
    days = t["daily_counts"]["rows"]
    assert len(days) == gold.DAILY_DAYS
    assert days[-1][0] == "2026-10-06"  # yesterday; today is still in progress
    assert sum(n for _, n in days) == 5  # in the window; small_recent and today fall on today
    assert 0 in {n for _, n in days}  # quiet days are present, not missing


def test_strongest_this_year(tables):
    t, _ = tables
    s = objects(t["strongest_this_year"])
    assert t["strongest_this_year"]["year"] == 2026
    assert [q["id"] for q in s[:2]] == ["this_year_big", "month_big"]
    assert "last_year" not in {q["id"] for q in s}


def test_japan_box(tables):
    t, _ = tables
    ids = {q["id"] for q in objects(t["japan_quakes"])}
    assert "japan" in ids and "chile" not in ids


def test_times_are_epoch_ms(tables):
    t, _ = tables
    q = next(q for q in objects(t["recent_quakes"]) if q["id"] == "week")
    assert q["time"] == int((NOW - timedelta(days=5)).timestamp() * 1000)


def test_pipeline_health_from_dq_history(tmp_path, monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    now = datetime.now(timezone.utc)  # the checks run against the real clock
    build_clean(tmp_path, [quake(f"q{i}", now - timedelta(hours=i + 1)) for i in range(3)])
    args = ["--data-dir", str(tmp_path), "--freshness-hours", "1e9", "--known-issues", "none.csv"]
    assert checks.main(args) == 0
    t = gold.build_tables(tmp_path, now)
    [run] = objects(t["pipeline_runs"])
    assert run["published"] is True and run["errors"] == 0 and run["row_count"] == 3
    assert {c["check"] for c in t["latest_checks"]["checks"]} >= {"schema matches", "data is fresh"}


def test_no_dq_history_skips_health_tables(tables):
    t, _ = tables
    assert "pipeline_runs" not in t


def test_write_parquet_and_json(tables, tmp_path):
    t, data_dir = tables
    site = tmp_path / "site_data"
    gold.write(t, data_dir, site, NOW)
    assert pq.read_table(data_dir / "gold" / "recent_quakes.parquet").num_rows == 5
    summary = json.loads((site / "summary.json").read_text())
    assert summary["generated_at"].startswith("2026-10-07") and summary["last_7d"] == 4
