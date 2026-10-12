"""Tests for the JMA data quality checks."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from jma_samples import catalog_line, daily_line, feed_entry, write_catalog_zip, write_daily, write_feed

from quake_pipeline.quality import checks
from quake_pipeline.transform import clean, clean_jma, match

NOW = datetime(2024, 1, 5, 0, 0, tzinfo=timezone.utc)
NO_FILE = Path("does-not-exist.csv")


def build(data_dir: Path, days=("20240101", "20240102", "20240103"), felt_shindo="3", with_usgs=True) -> None:
    raw = data_dir / "raw" / "jma"
    write_catalog_zip(
        raw,
        "h2023.zip",
        [
            catalog_line(datetime(2023, 3, 1, 21), 38.1, 142.0, mag=5.4),
            catalog_line(datetime(2023, 3, 2, 9), 35.0, 140.0, mag=1.2),
        ],
    )
    for day in days:
        when = datetime.strptime(day, "%Y%m%d") + timedelta(hours=12)
        write_daily(
            raw, day, [daily_line(when, 36.0, 140.0), daily_line(when + timedelta(hours=1), 36.1, 140.1)]
        )
    write_feed(raw, "20240104T000000Z", [feed_entry("e1", "2024-01-04T10:00:00+09:00", maxi=felt_shindo)])
    clean_jma.build(data_dir)

    if with_usgs:  # the USGS twin of the M5.4
        row = {c: "" for c in clean.COLUMNS}
        t = "2023-03-01T12:00:01.000Z"
        row.update(id="us1", time=t, updated=t, latitude="38.15", longitude="142.05", depth="10", mag="5.6")
        d = data_dir / "raw" / "usgs" / "run_id=x"
        d.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([row], dtype=str).assign(_source="usgs", _ingested_at="x").to_parquet(d / "p.parquet")
        clean.build(data_dir)
        match.build(data_dir)


def run(data_dir, known=None):
    return {r.check: r for r in checks.run_checks(data_dir, NOW, known=known, dataset=checks.JMA)}


def test_good_jma_build_passes(tmp_path):
    build(tmp_path)
    results = run(tmp_path)
    assert [c for c, r in results.items() if not r.passed] == []
    assert results["not empty"].value == 8
    assert results["no missing days in daily lists"].observed == "3 days, 2024-01-01 to 2024-01-03"
    assert results["M5+ quakes also in USGS"].value == 1.0


def test_missing_day_fails(tmp_path):
    build(tmp_path, days=("20240101", "20240103"))
    r = run(tmp_path)["no missing days in daily lists"]
    assert r.blocking and "2024-01-02" in r.observed


def test_gap_between_catalog_and_daily_lists_fails(tmp_path):
    build(tmp_path, days=("20240102", "20240103"))  # the catalog ends Dec 31; Jan 1 is missing
    r = run(tmp_path)["no missing days in daily lists"]
    assert r.blocking and "2024-01-01" in r.observed


def test_invalid_shindo_in_felt_feed_fails(tmp_path):
    build(tmp_path, felt_shindo="9")
    assert run(tmp_path)["felt shindo is a JMA level"].blocking


def test_unmatched_big_quake_only_warns(tmp_path):
    build(tmp_path, with_usgs=False)
    (tmp_path / "clean" / "usgs").mkdir(parents=True)  # no matches file: check skipped
    assert "M5+ quakes also in USGS" not in run(tmp_path)

    build(tmp_path)  # now with USGS, but drop the match
    matches = tmp_path / "clean" / match.OUT_NAME / "matches.parquet"
    pd.read_parquet(matches).iloc[0:0].to_parquet(matches)
    r = run(tmp_path)["M5+ quakes also in USGS"]
    assert not r.passed and not r.blocking and r.value == 0.0


def test_freshness_allows_the_two_day_publishing_lag(tmp_path):
    build(tmp_path)
    results = {
        r.check: r
        for r in checks.run_checks(tmp_path, NOW + timedelta(days=3), dataset=checks.JMA)  # ~4.6 days old
    }
    assert results["data is fresh"].blocking


def test_known_issues_only_apply_to_their_own_dataset(tmp_path):
    build(tmp_path)
    known = {"depth plausible": {"jma-20990101T000000.00-abcd", "us_not_jma"}}
    r = run(tmp_path, known)["known issues still apply"]
    assert "jma-20990101T000000.00-abcd" in r.observed and "us_not_jma" not in r.observed


def test_cli_jma_writes_its_own_history_and_baseline(tmp_path, monkeypatch):
    build(tmp_path)
    monkeypatch.setenv("QUAKE_RUN_ID", "20240105T000000Z")
    rc = checks.main(
        [
            "--dataset",
            "jma",
            "--data-dir",
            str(tmp_path),
            "--freshness-hours",
            "1e9",
            "--known-issues",
            str(NO_FILE),
        ]
    )
    assert rc == 0
    df = pd.read_parquet(tmp_path / "dq" / "results" / "dq_20240105T000000Z_jma.parquet")
    assert (df["dataset"] == "jma").all() and (df["run_id"] == "20240105T000000Z").all()
    assert json.loads((tmp_path / "state" / "quality_jma.json").read_text())["row_count"] == 8
    assert not (tmp_path / "state" / "quality.json").exists()  # the USGS baseline is untouched
