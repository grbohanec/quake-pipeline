"""Tests for the JMA ingester (offline: a fake JMA site stands in for the real one)."""

import gzip
import json
from datetime import date, datetime, timezone

import pytest

from quake_pipeline.ingest import jma

NOW = datetime(2024, 1, 10, 0, 0, tzinfo=timezone.utc)  # 09:00 on Jan 10 in Japan -> lists up to Jan 8

INDEX = """<html><body>
<a href="data/hypo/h2022.zip">2022</a> <a href="data/hypo/h2023.zip">2023</a>
<a href="data/hypo/h1919.zip">1919-1950</a> <a href="data/hypo/h2023.zip">again</a>
</body></html>"""


class FakeResponse:
    def __init__(self, status=200, content=b"", headers=None):
        self.status_code = status
        self.content = content
        self.headers = headers or {}

    @property
    def text(self):
        return self.content.decode("utf-8")

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeJMA:
    """Serves the catalog index, zips (with HEAD), daily pages and the feed."""

    def __init__(self, missing_days=(), last_modified="Mon, 01 Jan 2024 00:00:00 GMT"):
        self.missing_days = set(missing_days)
        self.last_modified = last_modified
        self.gets: list[str] = []

    def get(self, url, timeout=None):
        self.gets.append(url)
        if url == jma.CATALOG_INDEX:
            return FakeResponse(content=INDEX.encode())
        if url.startswith(jma.CATALOG_BASE):
            return FakeResponse(content=b"PK-zip-" + url.encode())
        if url == jma.FEED_URL:
            return FakeResponse(content=json.dumps([{"eid": "1"}, {"eid": "2"}]).encode())
        day = url.rsplit("/", 1)[1][:8]
        if day in self.missing_days:
            return FakeResponse(404)
        return FakeResponse(content=f"<pre>list for {day}</pre>".encode())

    def head(self, url, timeout=None, allow_redirects=True):
        return FakeResponse(headers={"Content-Length": "123", "Last-Modified": self.last_modified})


def client(fake):
    return jma.JMAClient(session=fake, pause=0)


def test_catalog_files_listed_once_each():
    assert jma.catalog_files(INDEX) == ["h1919.zip", "h2022.zip", "h2023.zip"]


def test_first_run_backfills_from_start_to_two_days_ago():
    days = jma.days_to_fetch({}, date(2024, 1, 10), lookback_days=7, start=date(2024, 1, 1))
    assert days[0] == date(2024, 1, 1) and days[-1] == date(2024, 1, 8) and len(days) == 8


def test_later_runs_fetch_new_days_plus_lookback():
    state = {"daily_done_until": "2024-03-01"}
    days = jma.days_to_fetch(state, date(2024, 3, 5), lookback_days=3, start=date(2024, 1, 1))
    # New: Mar 2-3. Lookback: the last 3 available days (Mar 1-3) again for revisions.
    assert days == [date(2024, 3, 1), date(2024, 3, 2), date(2024, 3, 3)]


def test_run_saves_raw_files_and_state(tmp_path):
    fake = FakeJMA(missing_days={"20240105"})
    state = jma.run(tmp_path, catalog=True, daily=True, feed=True, client=client(fake), now=NOW)

    raw = tmp_path / "raw" / "jma"
    assert sorted(p.name for p in (raw / "catalog").iterdir()) == ["h1919.zip", "h2022.zip", "h2023.zip"]
    daily = sorted(p.name for p in (raw / "daily" / "2024").iterdir())
    assert daily[0] == "20240101.html.gz" and daily[-1] == "20240108.html.gz"
    assert "20240105.html.gz" not in daily  # a 404 is skipped, not saved
    page = gzip.decompress((raw / "daily" / "2024" / "20240102.html.gz").read_bytes())
    assert page == b"<pre>list for 20240102</pre>"  # saved exactly as served
    [feed] = (raw / "feed").glob("run_id=*/list.json.gz")
    assert len(json.loads(gzip.decompress(feed.read_bytes()))) == 2

    assert state["daily_done_until"] == "2024-01-08"
    assert state["last_run"] == {"catalog_files": 3, "daily_pages": 7, "feed_bulletins": 2}
    assert json.loads((tmp_path / "state" / "jma.json").read_text())["catalog"]["h2023.zip"]["size"] == "123"


def test_unchanged_catalog_files_are_not_downloaded_again(tmp_path):
    jma.run(tmp_path, catalog=True, daily=False, feed=False, client=client(FakeJMA()), now=NOW)
    fake = FakeJMA()
    state = jma.run(tmp_path, catalog=True, daily=False, feed=False, client=client(fake), now=NOW)
    assert state["last_run"]["catalog_files"] == 0
    assert not any(u.endswith(".zip") for u in fake.gets)


def test_republished_catalog_file_is_downloaded_again(tmp_path):
    jma.run(tmp_path, catalog=True, daily=False, feed=False, client=client(FakeJMA()), now=NOW)
    fake = FakeJMA(last_modified="Tue, 02 Jan 2024 00:00:00 GMT")
    state = jma.run(tmp_path, catalog=True, daily=False, feed=False, client=client(fake), now=NOW)
    assert state["last_run"]["catalog_files"] == 3


def test_empty_catalog_index_is_an_error(tmp_path):
    fake = FakeJMA()
    fake.get = lambda url, timeout=None: FakeResponse(content=b"<html>redesigned page</html>")
    with pytest.raises(RuntimeError, match="page layout"):
        jma.run(tmp_path, catalog=True, daily=False, feed=False, client=client(fake), now=NOW)


def test_cli_requires_a_mode():
    with pytest.raises(SystemExit):
        jma.main([])
