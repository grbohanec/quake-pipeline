"""Tests for the JMA clean step: parsing all three formats and stitching them together."""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest
from jma_samples import (
    catalog_line,
    daily_line,
    feed_entry,
    write_catalog_zip,
    write_daily,
    write_feed,
)

from quake_pipeline.transform import clean_jma


def read(data_dir: Path, table: str = "jma") -> list[dict]:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    glob = (data_dir / "clean" / table / "**" / "*.parquet").as_posix()
    sql = f"SELECT * FROM read_parquet('{glob}', hive_partitioning = true) ORDER BY event_time"
    table = con.execute(sql).arrow()  # via Arrow: timestamps come back without needing pytz
    return (table.read_all() if hasattr(table, "read_all") else table).to_pylist()


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def raw(tmp_path):
    return tmp_path / "raw" / "jma"


def test_catalog_fields_are_parsed_to_spec(tmp_path, raw):
    write_catalog_zip(
        raw,
        "h2023.zip",
        [
            catalog_line(
                datetime(2023, 5, 5, 14, 42, 4, 500000),
                37.54,
                137.30,
                depth=12.34,
                mag=6.5,
                intensity="C",
                region="NOTO PENINSULA REGION",
            ),
        ],
    )
    clean_jma.build(tmp_path)
    [q] = read(tmp_path)
    assert q["event_time"] == utc(2023, 5, 5, 5, 42, 4, 500000)  # 14:42 JST = 05:42 UTC
    assert q["latitude"] == pytest.approx(37.54, abs=1e-4)
    assert q["longitude"] == pytest.approx(137.30, abs=1e-4)
    assert q["depth_km"] == pytest.approx(12.34)
    assert (q["mag"], q["mag_type"]) == (6.5, "Mj")
    assert q["max_intensity"] == "6-"  # code C = shindo 6 lower
    assert (q["status"], q["source"], q["agency"]) == ("final", "jma_catalog", "J")
    assert q["region"] == q["region_en"] == "NOTO PENINSULA REGION"
    assert q["year"] == 2023 and q["event_id"].startswith("jma-20230505T054204.50-")


@pytest.mark.parametrize(
    "mag,expected",
    [(4.5, 4.5), (0.5, 0.5), (-0.3, -0.3), (-1.0, -1.0), (-1.9, -1.9), (-2.5, -2.5), (-3.0, -3.0)],
)
def test_catalog_magnitude_codes(tmp_path, raw, mag, expected):
    write_catalog_zip(raw, "h2023.zip", [catalog_line(datetime(2023, 1, 1, 12), 35.0, 140.0, mag=mag)])
    clean_jma.build(tmp_path)
    assert read(tmp_path)[0]["mag"] == pytest.approx(expected)


def test_catalog_codes_for_event_type_far_quakes_and_missing_magnitude(tmp_path, raw):
    write_catalog_zip(
        raw,
        "h2023.zip",
        [
            catalog_line(datetime(2023, 1, 1, 9, 0, 1), 35.0, 140.0, mag=None, subsidiary="3"),
            catalog_line(datetime(2023, 6, 1, 9), -20.5, -70.25, mag=7.0, mag_type="W", agency="U"),
        ],
    )
    summary = clean_jma.build(tmp_path)
    blast, chile = read(tmp_path)
    assert blast["event_type"] == "artificial" and blast["mag"] is None  # kept: a real, located event
    assert chile["latitude"] == pytest.approx(-20.5) and chile["longitude"] == pytest.approx(-70.25)
    assert (chile["mag_type"], chile["agency"]) == ("Mw", "U")
    assert summary["dropped"] == {}


def test_fixed_hypocenter_blank_decimals(tmp_path, raw):
    line = catalog_line(datetime(1923, 9, 1, 11, 58, 32), 35.33, 139.25, depth=23, mag=7.9, intensity="6")
    # Fixed hypocenters leave the decimal digits of seconds and minutes blank.
    line = line[:15] + "  " + line[17:26] + "  " + line[28:]
    write_catalog_zip(raw, "h1919.zip", [line])
    clean_jma.build(tmp_path)
    [q] = read(tmp_path)
    assert q["event_time"] == utc(1923, 9, 1, 2, 58, 32)  # JST fixed +9h, no daylight saving applied
    assert q["latitude"] == pytest.approx(35 + 19 / 60)
    assert q["max_intensity"] == "6"


def test_daily_lists_parse_and_get_english_names(tmp_path, raw):
    write_catalog_zip(
        raw,
        "h2023.zip",
        [catalog_line(datetime(2023, 5, 5, 14), 37.54, 137.30, region="NOTO PENINSULA REGION")],
    )
    write_daily(
        raw,
        "20240101",
        [
            # One cell away from the catalog quake: named from the neighbouring cell.
            daily_line(
                datetime(2024, 1, 1, 16, 10, 22, 500000), 37.5, 137.27, mag="7.6", region="石川県能登地方"
            ),
            daily_line(datetime(2024, 1, 1, 20, 0, 0), 32.6433, 131.985, depth="-", mag="-", region="日向灘"),
        ],
        encoding="shift_jis",  # older JMA pages are Shift_JIS
    )
    clean_jma.build(tmp_path)
    _, noto, hyuga = read(tmp_path)
    assert noto["event_time"] == utc(2024, 1, 1, 7, 10, 22, 500000)
    assert (noto["status"], noto["source"], noto["mag"]) == ("preliminary", "jma_daily", 7.6)
    assert noto["region"] == "石川県能登地方" and noto["region_en"] == "NOTO PENINSULA REGION"
    assert noto["latitude"] == pytest.approx(37.5, abs=1e-3)
    assert hyuga["mag"] is None and hyuga["depth_km"] is None  # "-" means not determined
    assert hyuga["region"] == "日向灘"  # the page footer after the last line is not part of it
    assert hyuga["region_en"] is None  # no catalog quakes nearby to borrow a name from


def test_catalog_replaces_daily_lists_for_the_years_it_covers(tmp_path, raw):
    write_catalog_zip(raw, "h2023.zip", [catalog_line(datetime(2023, 12, 31, 23), 35.0, 140.0)])
    write_daily(raw, "20231231", [daily_line(datetime(2023, 12, 31, 23), 35.0, 140.0)])  # now in the catalog
    write_daily(raw, "20240101", [daily_line(datetime(2024, 1, 1, 1), 35.0, 140.0)])
    summary = clean_jma.build(tmp_path)
    assert [q["status"] for q in read(tmp_path)] == ["final", "preliminary"]
    assert summary["catalog_covers_until"] == "20240101"


def test_overlapping_catalog_files_are_deduplicated(tmp_path, raw):
    line = catalog_line(datetime(1997, 9, 30, 12), 35.0, 140.0)
    write_catalog_zip(raw, "h199701.zip", [line])
    write_catalog_zip(raw, "h199710.zip", [line])
    summary = clean_jma.build(tmp_path)
    assert len(read(tmp_path)) == 1 and summary["duplicates_removed"] == 1


def test_felt_feed_keeps_latest_bulletin_per_event(tmp_path, raw):
    write_daily(raw, "20261010", [daily_line(datetime(2026, 10, 10, 1), 35.0, 140.0)])
    write_feed(
        raw,
        "20261011T000000Z",
        [
            feed_entry(
                "e1", "2026-10-11T15:49:00+09:00", mag="2.3", maxi="1", rdt="2026-10-11T15:53:00+09:00"
            ),
            feed_entry(
                "e2", "2026-10-11T16:00:00+09:00", cod="", maxi="2"
            ),  # intensity-only flash: no location
            feed_entry("e3", "2026-10-11T17:00:00+09:00", mag="Ｍ不明", maxi="5-", cod="+35.0+140.1-410000/"),
        ],
    )
    write_feed(
        raw,
        "20261012T000000Z",
        [feed_entry("e1", "2026-10-11T15:49:00+09:00", mag="2.4", maxi="2", rdt="2026-10-11T16:30:00+09:00")],
    )
    summary = clean_jma.build(tmp_path)
    e1, e3 = read(tmp_path, "jma_felt")
    assert (e1["report_id"], e1["mag"], e1["max_intensity"]) == ("e1", 2.4, "2")  # revised bulletin wins
    assert e1["event_time"] == utc(2026, 10, 11, 6, 49) and e1["depth_km"] == pytest.approx(10)
    assert e1["region_en"] == "Plain Area of Southern Miyazaki Prefecture"
    assert e3["mag"] is None and e3["depth_km"] == pytest.approx(410) and e3["max_intensity"] == "5-"
    assert summary["felt_rows"] == 2


def test_missing_raw_files_stop_the_build(tmp_path):
    with pytest.raises(SystemExit, match="quake-ingest-jma"):
        clean_jma.build(tmp_path)


def test_rebuild_replaces_previous_output(tmp_path, raw):
    write_catalog_zip(raw, "h2023.zip", [catalog_line(datetime(2023, 1, 1, 12), 35.0, 140.0)])
    clean_jma.build(tmp_path)
    write_catalog_zip(raw, "h2023.zip", [catalog_line(datetime(2023, 1, 1, 12), 36.0, 140.0)])
    clean_jma.build(tmp_path)
    assert [q["latitude"] for q in read(tmp_path)] == [pytest.approx(36.0)]
    assert not (tmp_path / "clean" / "_jma_building").exists()
