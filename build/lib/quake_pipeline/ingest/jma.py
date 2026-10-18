"""Pull earthquake data from the Japan Meteorological Agency (JMA) into the raw layer.

JMA (Kishōchō, 気象庁) locates far more small Japanese earthquakes than USGS,
but publishes them in three different forms, each covering a different period:

    catalog   final, reviewed hypocenters        1919 -> end of the last published year
              yearly zip files of fixed-width text (published with a lag of ~2 years)
    daily     preliminary hypocenter lists       Jan 2024 -> 2 days ago
              one HTML page per day
    feed      felt earthquakes, with shindo      the last ~10 days, near real time
              (震度, seismic intensity: how strongly the shaking was felt, JMA's 0-7 scale)

The ingester saves each one exactly as JMA serves it (zips as-is, pages and JSON
gzipped), so parsing can be fixed and re-run without downloading again. The clean
step (transform/clean_jma.py) stitches them into one history.

    quake-ingest-jma --catalog            download new or changed catalog zips
    quake-ingest-jma --daily              daily lists not downloaded yet, plus the last week again
    quake-ingest-jma --feed               one snapshot of the felt-earthquake feed

Sources (pages are in Japanese; the catalog format is documented in English):
    https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html
    https://www.data.jma.go.jp/eqev/data/bulletin/data/format/hypfmt_e.html
    https://www.data.jma.go.jp/eqev/data/daily_map/index.html
    https://www.jma.go.jp/bosai/map.html#contents=earthquake_map
"""

from __future__ import annotations

import argparse
import gzip
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from quake_pipeline.ingest.usgs import load_state, make_session, save_state

log = logging.getLogger(__name__)

SOURCE = "jma"
JST = timezone(timedelta(hours=9))  # JMA publishes everything in Japan time (no daylight saving)

CATALOG_INDEX = "https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html"
CATALOG_BASE = "https://www.data.jma.go.jp/eqev/data/bulletin/data/hypo/"
DAILY_URL = "https://www.data.jma.go.jp/eqev/data/daily_map/{day:%Y%m%d}.html"
FEED_URL = "https://www.jma.go.jp/bosai/quake/data/list.json"

DAILY_FIRST_DAY = date(2024, 1, 1)  # the catalog ends at 2023; daily lists cover everything after
DAILY_LAG_DAYS = 2  # JMA publishes a day's list two days later
DEFAULT_LOOKBACK_DAYS = 7  # preliminary lists can be revised; fetch the last week again every run
REQUEST_PAUSE_S = 0.5  # be polite: a backfill is ~1,000 pages from a government site

CATALOG_FILE = re.compile(r"data/hypo/(h\d{4,6}\.zip)")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


class JMAClient:
    def __init__(
        self, session: requests.Session | None = None, timeout: int = 60, pause: float = REQUEST_PAUSE_S
    ):
        self.session = session or make_session()
        self.timeout = timeout
        self.pause = pause

    def get(self, url: str) -> requests.Response:
        resp = self.session.get(url, timeout=self.timeout)
        if self.pause:
            time.sleep(self.pause)
        return resp

    def head(self, url: str) -> requests.Response:
        resp = self.session.head(url, timeout=self.timeout, allow_redirects=True)
        if self.pause:
            time.sleep(self.pause)
        return resp


# --- Catalog -----------------------------------------------------------------


def catalog_files(index_html: str) -> list[str]:
    """Zip file names linked from the catalog index page, e.g. ['h1919.zip', ..., 'h2023.zip']."""
    return sorted(set(CATALOG_FILE.findall(index_html)))


def ingest_catalog(client: JMAClient, raw_dir: Path, state: dict) -> int:
    """Download catalog zips that are new or changed since the last run.

    Published years rarely change, but JMA does re-release files (older years are
    being relocated), so each file's size and Last-Modified header are remembered
    and compared with a cheap HEAD request instead of downloading everything again.
    """
    resp = client.get(CATALOG_INDEX)
    resp.raise_for_status()
    names = catalog_files(resp.text)
    if not names:
        raise RuntimeError(f"No catalog files found on {CATALOG_INDEX}; has the page layout changed?")

    seen: dict = state.setdefault("catalog", {})
    downloaded = 0
    for name in names:
        url = CATALOG_BASE + name
        path = raw_dir / "catalog" / name
        head = client.head(url)
        head.raise_for_status()
        version = {
            "size": head.headers.get("Content-Length"),
            "last_modified": head.headers.get("Last-Modified"),
        }
        if path.exists() and seen.get(name) == version:
            continue
        r = client.get(url)
        r.raise_for_status()
        _write_atomic(path, r.content)
        seen[name] = version
        downloaded += 1
        log.info("catalog %s  %.1f MB", name, len(r.content) / 1e6)
    log.info("Catalog: %d files listed, %d downloaded", len(names), downloaded)
    return downloaded


# --- Daily lists -------------------------------------------------------------


def daily_path(raw_dir: Path, day: date) -> Path:
    return raw_dir / "daily" / f"{day:%Y}" / f"{day:%Y%m%d}.html.gz"


def days_to_fetch(state: dict, today_jst: date, lookback_days: int, start: date) -> list[date]:
    """Every day after the last one fetched, plus the last `lookback_days` again for revisions."""
    last_available = today_jst - timedelta(days=DAILY_LAG_DAYS)
    done = date.fromisoformat(state["daily_done_until"]) if "daily_done_until" in state else None
    first = (
        start
        if done is None
        else min(done + timedelta(days=1), last_available - timedelta(days=lookback_days - 1))
    )
    first = max(first, start)
    return [first + timedelta(days=i) for i in range((last_available - first).days + 1)]


def ingest_daily(
    client: JMAClient,
    raw_dir: Path,
    state: dict,
    state_path: Path,
    today_jst: date,
    lookback_days: int,
    start: date,
) -> int:
    fetched = 0
    for day in days_to_fetch(state, today_jst, lookback_days, start):
        resp = client.get(DAILY_URL.format(day=day))
        if resp.status_code == 404:
            # Normal for the newest day if JMA is running late; anything older is worth a look.
            log.warning("No daily list for %s (404)", day)
            continue
        resp.raise_for_status()
        _write_atomic(daily_path(raw_dir, day), gzip.compress(resp.content, mtime=0))
        fetched += 1
        done = state.get("daily_done_until")
        if done is None or day.isoformat() > done:
            state["daily_done_until"] = day.isoformat()
        if fetched % 50 == 0:
            log.info("daily lists: %d fetched, up to %s", fetched, day)
            save_state(state_path, state)  # an interrupted backfill resumes from here
    log.info("Daily lists: %d pages fetched (up to %s)", fetched, state.get("daily_done_until"))
    return fetched


# --- Felt-earthquake feed ----------------------------------------------------


def ingest_feed(client: JMAClient, raw_dir: Path, run_id: str) -> int:
    """Save one snapshot of the feed. It only holds ~10 days, so daily snapshots build the archive."""
    resp = client.get(FEED_URL)
    resp.raise_for_status()
    entries = resp.json()  # fail now, not in the clean step, if JMA sent something that isn't JSON
    if not isinstance(entries, list):
        raise RuntimeError(f"Unexpected feed format from {FEED_URL}: {type(entries).__name__}")
    path = raw_dir / "feed" / f"run_id={run_id}" / "list.json.gz"
    _write_atomic(path, gzip.compress(resp.content, mtime=0))
    log.info("Feed: %d bulletins saved", len(entries))
    return len(entries)


# --- Entry point -------------------------------------------------------------


def run(
    data_dir: Path,
    catalog: bool,
    daily: bool,
    feed: bool,
    client: JMAClient | None = None,
    now: datetime | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    daily_start: date = DAILY_FIRST_DAY,
) -> dict:
    client = client or JMAClient()
    now = now or _now_utc()
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    raw_dir = data_dir / "raw" / SOURCE
    state_path = data_dir / "state" / f"{SOURCE}.json"
    state = load_state(state_path)

    counts = {}
    if catalog:
        counts["catalog_files"] = ingest_catalog(client, raw_dir, state)
        save_state(state_path, state)
    if daily:
        today_jst = now.astimezone(JST).date()
        counts["daily_pages"] = ingest_daily(
            client, raw_dir, state, state_path, today_jst, lookback_days, daily_start
        )
        save_state(state_path, state)
    if feed:
        counts["feed_bulletins"] = ingest_feed(client, raw_dir, run_id)

    state |= {"last_run_id": run_id, "last_run": counts}
    save_state(state_path, state)
    return state


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ingest JMA earthquake data into the raw layer.")
    p.add_argument("--catalog", action="store_true", help="download new or changed yearly catalog files")
    p.add_argument("--daily", action="store_true", help="download daily hypocenter lists")
    p.add_argument("--feed", action="store_true", help="save a snapshot of the felt-earthquake feed")
    p.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="daily: also re-download this many recent days, for revisions",
    )
    p.add_argument("--daily-start", default=DAILY_FIRST_DAY.isoformat(), help="daily: first day to backfill")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    args = p.parse_args(argv)
    if not (args.catalog or args.daily or args.feed):
        p.error("choose at least one of --catalog, --daily, --feed")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(
        args.data_dir,
        args.catalog,
        args.daily,
        args.feed,
        lookback_days=args.lookback_days,
        daily_start=date.fromisoformat(args.daily_start),
    )


if __name__ == "__main__":
    main()
