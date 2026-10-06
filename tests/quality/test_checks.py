"""Tests for the data quality checks."""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from quake_pipeline.quality import checks
from quake_pipeline.transform import clean

NOW = datetime(2024, 3, 6, 0, 0, tzinfo=timezone.utc)  # a day after the sample events
LOOSE = checks.Thresholds()
NO_FILE = Path("does-not-exist.csv")


def raw_row(**kw):
    row = {c: "" for c in clean.COLUMNS}
    row.update(
        id="us001",
        time="2024-03-05T10:00:00.000Z",
        updated="2024-03-05T10:05:00.000Z",
        latitude="35.6",
        longitude="139.7",
        depth="10",
        mag="3.2",
        type="earthquake",
    )
    row.update(kw)
    return row


def build(data_dir: Path, rows) -> None:
    """Write raw rows and run the real cleaning step, as the pipeline does."""
    d = data_dir / "raw" / "usgs" / "run_id=20240306T000000Z"
    d.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, dtype=str).assign(_source="usgs", _ingested_at="20240306T000000Z")
    df.to_parquet(d / f"part{len(list(d.iterdir()))}.parquet", index=False)
    clean.build(data_dir)


def good_rows(n=3):
    return [raw_row(id=f"us{i:03d}") for i in range(n)]


def by_name(results):
    return {r.check: r for r in results}


def test_good_build_passes(tmp_path):
    build(tmp_path, good_rows())
    results = checks.run_checks(tmp_path, NOW, LOOSE)
    assert [r.check for r in results if not r.passed] == []
    assert by_name(results)["not empty"].value == 3


def test_missing_clean_layer_fails(tmp_path):
    [r] = checks.run_checks(tmp_path, NOW, LOOSE)
    assert r.blocking


def test_duplicate_event_ids_fail(tmp_path):
    build(tmp_path, good_rows())
    part = next((tmp_path / "clean" / "usgs" / "year=2024").glob("*.parquet"))
    shutil.copy(part, part.with_name("copy.parquet"))  # simulate a bad merge
    r = by_name(checks.run_checks(tmp_path, NOW, LOOSE))["event_id unique"]
    assert r.blocking and r.value == 3


def test_schema_drift_fails_and_stops(tmp_path):
    out = tmp_path / "clean" / "usgs" / "year=2024"
    out.mkdir(parents=True)
    duckdb.sql(f"COPY (SELECT 'x' AS event_id) TO '{(out / 'a.parquet').as_posix()}'")
    results = checks.run_checks(tmp_path, NOW, LOOSE)
    assert results[0].check == "schema matches" and results[0].blocking
    assert "missing" in results[0].observed
    assert "not empty" not in by_name(results)  # nothing else runs on a broken schema


def test_stale_data_fails(tmp_path):
    build(tmp_path, good_rows())
    later = datetime(2024, 3, 10, tzinfo=timezone.utc)
    r = by_name(checks.run_checks(tmp_path, later, LOOSE))["data is fresh"]
    assert r.blocking and r.value > 48


def test_future_events_fail(tmp_path):
    build(tmp_path, [*good_rows(), raw_row(id="future", time="2024-03-07T00:00:00.000Z")])
    assert by_name(checks.run_checks(tmp_path, NOW, LOOSE))["no events in the future"].blocking


def test_implausible_depth_only_warns(tmp_path):
    build(tmp_path, [*good_rows(), raw_row(id="deep", depth="900")])
    r = by_name(checks.run_checks(tmp_path, NOW, LOOSE))["depth plausible"]
    assert not r.passed and not r.blocking


def test_row_count_drop_fails(tmp_path):
    build(tmp_path, good_rows(3))
    (tmp_path / "state" / checks.BASELINE_FILE).write_text(json.dumps({"row_count": 100}))
    r = by_name(checks.run_checks(tmp_path, NOW, LOOSE))["row count did not drop"]
    assert r.blocking and r.value == pytest.approx(-0.97)


def test_first_run_sets_baseline_without_failing(tmp_path):
    build(tmp_path, good_rows())
    r = by_name(checks.run_checks(tmp_path, NOW, LOOSE))["row count did not drop"]
    assert r.passed and "first run" in r.observed


def test_high_rejection_rate_fails(tmp_path):
    # e.g. USGS renames "mag": every row loses its magnitude and gets dropped by cleaning
    build(tmp_path, [*good_rows(2), *(raw_row(id=f"bad{i}", mag="") for i in range(3))])
    r = by_name(checks.run_checks(tmp_path, NOW, LOOSE))["cleaning rejected few rows"]
    assert r.blocking and r.value == pytest.approx(0.6)


def test_main_success_writes_history_and_baseline(tmp_path, monkeypatch):
    build(tmp_path, good_rows())
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    rc = checks.main(
        ["--data-dir", str(tmp_path), "--freshness-hours", "1e9", "--known-issues", str(NO_FILE)]
    )
    assert rc == 0
    assert json.loads((tmp_path / "state" / checks.BASELINE_FILE).read_text())["row_count"] == 3
    [hist] = (tmp_path / "dq" / "results").glob("dq_*.parquet")
    df = pd.read_parquet(hist)
    assert df["passed"].all() and df.loc[df["check"] == "not empty", "value"].item() == 3
    assert "Data quality: passed" in summary.read_text()


def test_main_failure_exits_1_and_keeps_old_baseline(tmp_path, monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    build(tmp_path, good_rows())
    baseline = tmp_path / "state" / checks.BASELINE_FILE
    baseline.write_text(json.dumps({"row_count": 100}))
    rc = checks.main(
        ["--data-dir", str(tmp_path), "--freshness-hours", "1e9", "--known-issues", str(NO_FILE)]
    )
    assert rc == 1
    assert json.loads(baseline.read_text())["row_count"] == 100  # a bad run never moves the baseline
    assert list((tmp_path / "dq" / "results").glob("dq_*.parquet"))  # failures are recorded too


def write_known(tmp_path, rows):
    path = tmp_path / "known_issues.csv"
    lines = ["check,event_id,reason,acknowledged_on"] + [f"{c},{e},{r},2026-10-06" for c, e, r in rows]
    path.write_text("\n".join(lines) + "\n")
    return path


def test_known_issue_is_skipped_but_new_ones_still_fire(tmp_path):
    build(tmp_path, [*good_rows(), raw_row(id="odd1", depth="900"), raw_row(id="odd2", depth="950")])
    known = checks.load_known_issues(write_known(tmp_path, [("depth plausible", "odd1", "reviewed")]))
    r = by_name(checks.run_checks(tmp_path, NOW, LOOSE, known))["depth plausible"]
    assert not r.passed and r.value == 1  # odd2 is new, so it still counts
    assert "1 known, acknowledged" in r.observed


def test_all_known_issues_acknowledged_passes(tmp_path):
    build(tmp_path, [*good_rows(), raw_row(id="odd1", depth="900")])
    known = checks.load_known_issues(write_known(tmp_path, [("depth plausible", "odd1", "reviewed")]))
    results = by_name(checks.run_checks(tmp_path, NOW, LOOSE, known))
    assert results["depth plausible"].passed
    assert "known issues still apply" not in results


def test_stale_known_issue_is_reported(tmp_path):
    build(tmp_path, good_rows())
    known = checks.load_known_issues(
        write_known(tmp_path, [("depth plausible", "fixed_upstream", "reviewed")])
    )
    r = by_name(checks.run_checks(tmp_path, NOW, LOOSE, known))["known issues still apply"]
    assert not r.passed and not r.blocking and "fixed_upstream" in r.observed


def test_known_issues_file_rejects_typos(tmp_path):
    with pytest.raises(ValueError, match="unknown check"):
        checks.load_known_issues(write_known(tmp_path, [("depth plausibel", "x", "typo")]))
    path = tmp_path / "no_reason.csv"
    path.write_text("check,event_id,reason,acknowledged_on\ndepth plausible,x,,2026-10-06\n")
    with pytest.raises(ValueError, match="reason are required"):
        checks.load_known_issues(path)


def test_repo_known_issues_file_is_valid():
    path = Path(__file__).resolve().parents[2] / "quality" / "known_issues.csv"
    assert checks.load_known_issues(path)  # parses and is not empty
