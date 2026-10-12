"""Tests for linking JMA earthquakes to USGS earthquakes."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from jma_samples import catalog_line, write_catalog_zip

from quake_pipeline.transform import clean, clean_jma, match

T0 = datetime(2023, 3, 1, 12, 0, tzinfo=timezone.utc)
JST = timedelta(hours=9)


def usgs_row(id, when, lat, lon, mag="4.0"):
    row = {c: "" for c in clean.COLUMNS}
    t = when.strftime("%Y-%m-%dT%H:%M:%S.") + f"{when.microsecond // 1000:03d}Z"
    row.update(id=id, time=t, updated=t, latitude=str(lat), longitude=str(lon), depth="10", mag=mag)
    return row


def build(data_dir: Path, jma_quakes, usgs_quakes) -> pd.DataFrame:
    lines = [
        catalog_line((when + JST).replace(tzinfo=None), lat, lon, mag=mag)
        for when, lat, lon, mag in jma_quakes
    ]
    write_catalog_zip(data_dir / "raw" / "jma", "h2023.zip", lines)
    clean_jma.build(data_dir)
    d = data_dir / "raw" / "usgs" / "run_id=x"
    d.mkdir(parents=True, exist_ok=True)
    rows = [usgs_row(*q) for q in usgs_quakes]
    pd.DataFrame(rows, dtype=str).assign(_source="usgs", _ingested_at="x").to_parquet(d / "p.parquet")
    clean.build(data_dir)
    match.build(data_dir)
    return pd.read_parquet(data_dir / "clean" / match.OUT_NAME / "matches.parquet")


def test_same_quake_reported_by_both_agencies_is_linked(tmp_path):
    m = build(
        tmp_path,
        [(T0, 38.10, 142.00, 5.2)],
        [("us1", T0 + timedelta(seconds=2.5), 38.20, 142.10, "5.5")],
    )
    assert len(m) == 1
    r = m.iloc[0]
    assert r.usgs_event_id == "us1" and r.jma_event_id.startswith("jma-20230301T120000")
    assert r.time_diff_s == pytest.approx(2.5) and 10 < r.distance_km < 20
    assert r.mag_diff == pytest.approx(0.3)


def test_each_record_is_used_once(tmp_path):
    # A mainshock and an aftershock 8 s later at the same place, but USGS only has the mainshock:
    # it must link to the closer JMA record, not to both.
    m = build(
        tmp_path,
        [(T0, 38.1, 142.0, 6.0), (T0 + timedelta(seconds=8), 38.1, 142.0, 4.0)],
        [("us1", T0 + timedelta(seconds=1), 38.1, 142.0, "6.1")],
    )
    assert len(m) == 1 and m.iloc[0].time_diff_s == pytest.approx(1)


def test_too_far_apart_in_time_or_space_is_not_linked(tmp_path):
    m = build(
        tmp_path,
        [(T0, 38.1, 142.0, 5.0), (T0 + timedelta(hours=1), 35.0, 140.0, 5.0)],
        [
            ("late", T0 + timedelta(seconds=60), 38.1, 142.0, "5.0"),  # 60 s apart
            ("far", T0 + timedelta(hours=1), 33.0, 140.0, "5.0"),  # ~220 km apart
        ],
    )
    assert m.empty


def test_quakes_outside_japan_are_ignored(tmp_path):
    m = build(tmp_path, [(T0, -20.5, -70.25, 7.0)], [("chile", T0, -20.5, -70.25, "7.0")])
    assert m.empty
