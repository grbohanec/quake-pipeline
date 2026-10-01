"""Tests for USGS ingestion, using a fake USGS API so nothing hits the network."""

from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pytest

from quake_pipeline.ingest import usgs
from quake_pipeline.ingest.usgs import USGSClient, Window

UTC = timezone.utc
HEADER = "time,latitude,longitude,depth,mag,magType,id,updated,place,type"


def make_events(n, start, step, mag="3.1", updated=None):
    """n events spaced `step` apart starting at `start`."""
    events = []
    for i in range(n):
        t = start + i * step
        events.append({
            "time": t.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "id": f"us{i:07d}",
            "mag": mag,
            "updated": (updated or t).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        })
    return events


class FakeResponse:
    status_code = 200

    def __init__(self, text="", json_data=None):
        self.text = text
        self._json = json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


class FakeUSGS:
    """Mimics /count and /query, including the 20,000-event cap."""

    def __init__(self, events):
        self.events = events
        self.calls = []

    def _filter(self, params):
        start = usgs._parse_dt(params["starttime"])
        end = usgs._parse_dt(params["endtime"])
        min_mag = float(params["minmagnitude"])
        upd = usgs._parse_dt(params["updatedafter"]) if "updatedafter" in params else None
        out = []
        for e in self.events:
            t = usgs._parse_dt(e["time"])
            if not (start <= t < end) or float(e["mag"]) < min_mag:
                continue
            if upd and usgs._parse_dt(e["updated"]) <= upd:
                continue
            out.append(e)
        return out

    def get(self, url, params, timeout):
        params = {k: str(v) for k, v in params.items()}
        self.calls.append((url.rsplit("/", 1)[-1], params))
        rows = self._filter(params)
        if url.endswith("/count"):
            return FakeResponse(json_data={"count": len(rows)})
        if len(rows) > usgs.MAX_EVENTS_PER_QUERY:
            raise AssertionError("query exceeded the USGS 20,000 event limit")
        lines = [HEADER] + [
            f'{e["time"]},35.0,139.0,10,{e["mag"]},ml,{e["id"]},{e["updated"]},"Somewhere, Japan",earthquake'
            for e in rows
        ]
        return FakeResponse(text="\n".join(lines) + "\n")


def read_raw(out_dir):
    files = sorted(out_dir.rglob("*.parquet"))
    return pd.concat(pd.read_parquet(f) for f in files) if files else pd.DataFrame()


@pytest.fixture
def paths(tmp_path):
    return tmp_path / "raw" / "usgs", tmp_path / "state" / "usgs.json"


def test_small_window_is_one_query(paths):
    out_dir, state_path = paths
    start = datetime(2024, 1, 1, tzinfo=UTC)
    fake = FakeUSGS(make_events(100, start, timedelta(hours=1)))
    usgs.run(Window(start, start + timedelta(days=30)), 2.0, out_dir, state_path,
             client=USGSClient(session=fake))

    assert [c[0] for c in fake.calls] == ["count", "query"]
    df = read_raw(out_dir)
    assert len(df) == 100
    assert (df["_source"] == "usgs").all()


def test_large_window_is_split_under_limit(paths):
    out_dir, state_path = paths
    start = datetime(2020, 1, 1, tzinfo=UTC)
    # 50,000 events -> must be split into at least 3 queries
    fake = FakeUSGS(make_events(50_000, start, timedelta(minutes=10)))
    usgs.run(Window(start, start + timedelta(days=400)), 2.0, out_dir, state_path,
             client=USGSClient(session=fake))

    queries = [c for c in fake.calls if c[0] == "query"]
    assert len(queries) >= 3
    df = read_raw(out_dir)
    assert len(df) == 50_000
    assert df["id"].is_unique  # window edges don't double-count events


def test_min_magnitude_filter(paths):
    out_dir, state_path = paths
    start = datetime(2024, 1, 1, tzinfo=UTC)
    events = make_events(10, start, timedelta(hours=1), mag="1.5") + \
        [e | {"id": f"big{i}"} for i, e in enumerate(make_events(5, start, timedelta(hours=1), mag="2.4"))]
    fake = FakeUSGS(events)
    usgs.run(Window(start, start + timedelta(days=1)), 2.0, out_dir, state_path,
             client=USGSClient(session=fake))
    assert len(read_raw(out_dir)) == 5


def test_watermark_and_incremental(paths):
    out_dir, state_path = paths
    start = datetime(2024, 1, 1, tzinfo=UTC)
    old = make_events(20, start, timedelta(hours=1))
    fake = FakeUSGS(old)
    client = USGSClient(session=fake)
    state = usgs.run(Window(start, start + timedelta(days=2)), 2.0, out_dir, state_path, client=client)
    assert state["max_updated"] == "2024-01-01T19:00:00"

    # Later: one existing event gets revised, and 3 new ones arrive.
    revised = old[0] | {"mag": "3.5", "updated": "2024-01-03T00:00:00.000Z"}
    new = [e | {"id": f"new{i}"} for i, e in
           enumerate(make_events(3, start + timedelta(days=1), timedelta(hours=1)))]
    fake.events = [revised] + old[1:] + new

    state = usgs.run(Window(start, start + timedelta(days=2)), 2.0, out_dir, state_path,
                     updated_after=usgs._parse_dt(state["max_updated"]), client=client)
    assert state["last_run_events"] == 4  # only the revised + new events
    assert state["max_updated"] == "2024-01-03T00:00:00"


def test_query_always_sends_starttime_and_endtime(paths):
    # USGS silently defaults to "last 30 days" if starttime is missing.
    out_dir, state_path = paths
    start = datetime(1990, 1, 1, tzinfo=UTC)
    fake = FakeUSGS(make_events(3, start, timedelta(days=1)))
    usgs.run(Window(start, start + timedelta(days=5)), 2.0, out_dir, state_path,
             client=USGSClient(session=fake))
    for _, params in fake.calls:
        assert "starttime" in params and "endtime" in params


def test_incremental_requires_backfill_first(tmp_path):
    with pytest.raises(SystemExit):
        usgs.main(["--incremental", "--data-dir", str(tmp_path)])


class FailingAfter(FakeUSGS):
    """Fake API that crashes after a set number of queries, like a dropped connection."""

    def __init__(self, events, fail_after):
        super().__init__(events)
        self.fail_after = fail_after

    def get(self, url, params, timeout):
        if url.endswith("/query"):
            self.fail_after -= 1
            if self.fail_after < 0:
                raise ConnectionError("network dropped")
        return super().get(url, params, timeout)


def test_interrupted_backfill_resumes(paths):
    out_dir, state_path = paths
    start = datetime(2020, 1, 1, tzinfo=UTC)
    events = make_events(50_000, start, timedelta(minutes=10))
    window = Window(start, start + timedelta(days=400))

    with pytest.raises(ConnectionError):
        usgs.run(window, 2.0, out_dir, state_path, client=USGSClient(session=FailingAfter(events, 1)))
    first_part = len(read_raw(out_dir))
    assert 0 < first_part < 50_000

    fake = FakeUSGS(events)
    usgs.run(window, 2.0, out_dir, state_path, client=USGSClient(session=fake))
    df = read_raw(out_dir)
    assert len(df) == 50_000 and df["id"].is_unique  # nothing missed, nothing re-pulled

    # Running the same backfill again does nothing.
    fake.calls.clear()
    usgs.run(window, 2.0, out_dir, state_path, client=USGSClient(session=fake))
    assert fake.calls == []


def test_restart_ignores_saved_progress(paths):
    out_dir, state_path = paths
    start = datetime(2024, 1, 1, tzinfo=UTC)
    fake = FakeUSGS(make_events(10, start, timedelta(hours=1)))
    window = Window(start, start + timedelta(days=1))
    usgs.run(window, 2.0, out_dir, state_path, client=USGSClient(session=fake))
    state = usgs.run(window, 2.0, out_dir, state_path, client=USGSClient(session=fake), restart=True)
    assert state["last_run_events"] == 10


def test_new_backfill_start_is_not_skipped(paths):
    # A small test pull, then the full history: the second must not be
    # mistaken for "already done" just because the first finished recently.
    out_dir, state_path = paths
    old = datetime(2000, 1, 1, tzinfo=UTC)
    recent = datetime(2024, 1, 1, tzinfo=UTC)
    events = make_events(10, old, timedelta(days=1)) + \
        [e | {"id": f"r{i}"} for i, e in enumerate(make_events(5, recent, timedelta(hours=1)))]
    fake = FakeUSGS(events)
    end = recent + timedelta(days=1)

    usgs.run(Window(recent, end), 2.0, out_dir, state_path, client=USGSClient(session=fake))
    state = usgs.run(Window(old, end), 2.0, out_dir, state_path, client=USGSClient(session=fake))
    assert state["last_run_events"] == 15


class TimesOutOnBigRanges(FakeUSGS):
    """Like real USGS: /count gives up (504) when asked about too long a range."""

    def __init__(self, events, max_days):
        super().__init__(events)
        self.max_days = max_days

    def get(self, url, params, timeout):
        span = usgs._parse_dt(str(params["endtime"])) - usgs._parse_dt(str(params["starttime"]))
        if url.endswith("/count") and span > timedelta(days=self.max_days):
            import requests
            raise requests.exceptions.RetryError("too many 504 error responses")
        return super().get(url, params, timeout)


def test_long_backfill_is_cut_into_years():
    w = Window(datetime(1880, 1, 1, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC))
    years = w.by_year()
    assert len(years) == 147
    assert years[0].start.year == 1880 and years[-1].end == w.end
    assert all(a.end == b.start for a, b in zip(years, years[1:]))


def test_server_timeout_splits_window(paths):
    out_dir, state_path = paths
    start = datetime(2020, 1, 1, tzinfo=UTC)
    events = make_events(1000, start, timedelta(hours=6))
    fake = TimesOutOnBigRanges(events, max_days=100)  # even one year is "too big"
    usgs.run(Window(start, start + timedelta(days=300)), 2.0, out_dir, state_path,
             client=USGSClient(session=fake))
    df = read_raw(out_dir)
    assert len(df) == 1000 and df["id"].is_unique
