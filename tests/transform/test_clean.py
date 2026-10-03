"""Tests for the cleaned layer."""

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from quake_pipeline.transform import clean

RAW_COLS = list(clean.COLUMNS)


def raw_row(**kw):
    row = {c: "" for c in RAW_COLS}
    row.update(
        {
            "id": "us001",
            "time": "2024-03-05T10:00:00.000Z",
            "updated": "2024-03-05T10:05:00.000Z",
            "latitude": "35.6",
            "longitude": "139.7",
            "depth": "10",
            "mag": "3.2",
            "magType": "mb",
            "type": "earthquake",
            "place": "near Tokyo, Japan",
        }
    )
    row.update(kw)
    return row


def write_raw(data_dir: Path, run_id: str, rows):
    d = data_dir / "raw" / "usgs" / f"run_id={run_id}"
    d.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, dtype=str).assign(_source="usgs", _ingested_at=run_id)
    df.to_parquet(d / f"part{len(list(d.iterdir()))}.parquet", index=False)


def read_clean(data_dir: Path) -> pd.DataFrame:
    glob = (data_dir / "clean" / "usgs" / "**" / "*.parquet").as_posix()
    return duckdb.sql(f"SELECT * FROM read_parquet('{glob}', hive_partitioning = true)").df()


def test_types_and_column_names(tmp_path):
    write_raw(tmp_path, "20261001T000000Z", [raw_row(nst="12", gap="")])
    clean.build(tmp_path)
    df = read_clean(tmp_path)
    assert len(df) == 1
    r = df.iloc[0]
    assert r["event_id"] == "us001"
    assert r["mag"] == pytest.approx(3.2)
    assert r["depth_km"] == pytest.approx(10.0)
    assert r["nst"] == 12
    assert pd.isna(r["gap"])  # empty text became null, not 0
    assert r["event_time"].year == 2024
    assert r["year"] == 2024


def test_keeps_newest_revision(tmp_path):
    write_raw(tmp_path, "20261001T000000Z", [raw_row(mag="3.2", updated="2024-03-05T10:05:00.000Z")])
    write_raw(tmp_path, "20261002T000000Z", [raw_row(mag="3.6", updated="2024-03-09T00:00:00.000Z")])
    # An older copy downloaded later (e.g. overlapping backfill) must not win.
    write_raw(tmp_path, "20261003T000000Z", [raw_row(mag="3.2", updated="2024-03-05T10:05:00.000Z")])
    summary = clean.build(tmp_path)
    df = read_clean(tmp_path)
    assert len(df) == 1
    assert df.iloc[0]["mag"] == pytest.approx(3.6)
    assert summary["duplicates_removed"] == 2


def test_bad_rows_dropped_and_reported(tmp_path):
    write_raw(
        tmp_path,
        "20261001T000000Z",
        [
            raw_row(id="ok1"),
            raw_row(id="nomag", mag=""),
            raw_row(id="badlat", latitude="123"),
            raw_row(id="notime", time=""),
            raw_row(id="garbage", mag="abc"),  # unparseable -> null -> missing magnitude
        ],
    )
    summary = clean.build(tmp_path)
    assert list(read_clean(tmp_path)["event_id"]) == ["ok1"]
    assert summary["dropped"] == {
        "missing event_time": 1,
        "missing magnitude": 2,
        "latitude out of range": 1,
    }


def test_partitioned_by_year(tmp_path):
    write_raw(
        tmp_path,
        "20261001T000000Z",
        [
            raw_row(id="a", time="1923-09-01T02:58:00.000Z", updated="2020-01-01T00:00:00.000Z"),
            raw_row(id="b", time="2011-03-11T05:46:00.000Z", updated="2020-01-01T00:00:00.000Z"),
        ],
    )
    clean.build(tmp_path)
    out = tmp_path / "clean" / "usgs"
    assert sorted(p.name for p in out.iterdir()) == ["year=1923", "year=2011"]


def test_rebuild_replaces_previous_output(tmp_path):
    write_raw(tmp_path, "20261001T000000Z", [raw_row(id="a")])
    clean.build(tmp_path)
    write_raw(tmp_path, "20261002T000000Z", [raw_row(id="b")])
    clean.build(tmp_path)
    assert sorted(read_clean(tmp_path)["event_id"]) == ["a", "b"]
    assert not (tmp_path / "clean" / "_usgs_building").exists()


def test_no_raw_data_gives_clear_error(tmp_path):
    with pytest.raises(SystemExit, match="run the ingest first"):
        clean.build(tmp_path)
