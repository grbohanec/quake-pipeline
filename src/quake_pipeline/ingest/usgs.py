"""Pull earthquake events from the USGS FDSN Event API into the raw data layer.

Two modes:

* backfill     -- pull every event between two dates (e.g. 1900 -> today).
* incremental  -- pull only events that are new or were revised since the last run.

USGS caps a single query at 20,000 events, so large date ranges are split
automatically: we ask the /count endpoint how many events a window holds and
keep halving the window until each piece fits under the cap.

Raw files are written exactly as USGS returns them (every column kept as a
string) plus two metadata columns. Typing, cleaning and de-duplication happen
in the next layer, so the raw layer is always a faithful copy of the source.

API docs: https://earthquake.usgs.gov/fdsnws/event/1/
"""

from __future__ import annotations

import argparse
import io
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1"
MAX_EVENTS_PER_QUERY = 20_000  # hard limit enforced by USGS
SAFETY_MARGIN = 0.9  # stay a little under the cap in case events arrive mid-pull
MIN_WINDOW = timedelta(minutes=1)  # never split finer than this

DEFAULT_MIN_MAG = 2.0
DEFAULT_LOOKBACK_DAYS = 30  # how far back incremental runs look for revised events
SOURCE = "usgs"


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime

    def split(self) -> tuple["Window", "Window"]:
        mid = self.start + (self.end - self.start) / 2
        return Window(self.start, mid), Window(mid, self.end)

    def __str__(self) -> str:
        return f"{_iso(self.start)} -> {_iso(self.end)}"


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def make_session() -> requests.Session:
    """HTTP session that retries rate limits and server errors with backoff."""
    retry = Retry(
        total=6,
        backoff_factor=2,  # 2s, 4s, 8s, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "quake-pipeline (github.com/grbohanec)"
    return session


class USGSClient:
    def __init__(self, session: requests.Session | None = None, timeout: int = 120):
        self.session = session or make_session()
        self.timeout = timeout

    def _params(self, window: Window, min_mag: float, updated_after: datetime | None) -> dict:
        # USGS defaults starttime to "30 days ago" if omitted, so always send both ends.
        params = {
            "starttime": _iso(window.start),
            "endtime": _iso(window.end),
            "minmagnitude": min_mag,
            "eventtype": "earthquake",
        }
        if updated_after is not None:
            params["updatedafter"] = _iso(updated_after)
        return params

    def count(self, window: Window, min_mag: float, updated_after: datetime | None = None) -> int:
        params = self._params(window, min_mag, updated_after) | {"format": "geojson"}
        resp = self.session.get(f"{BASE_URL}/count", params=params, timeout=self.timeout)
        resp.raise_for_status()
        return int(resp.json()["count"])

    def query(self, window: Window, min_mag: float, updated_after: datetime | None = None) -> pd.DataFrame:
        params = self._params(window, min_mag, updated_after) | {
            "format": "csv",
            "orderby": "time-asc",
            "limit": MAX_EVENTS_PER_QUERY,
        }
        resp = self.session.get(f"{BASE_URL}/query", params=params, timeout=self.timeout)
        resp.raise_for_status()
        if not resp.text.strip():
            return pd.DataFrame()
        # Keep everything as text in the raw layer; typing happens downstream.
        return pd.read_csv(io.StringIO(resp.text), dtype=str, keep_default_na=False)

    def plan_windows(
        self, window: Window, min_mag: float, updated_after: datetime | None = None
    ) -> Iterator[tuple[Window, int]]:
        """Yield (window, event_count) pieces that each fit under the USGS limit."""
        limit = int(MAX_EVENTS_PER_QUERY * SAFETY_MARGIN)
        stack = [window]
        while stack:
            w = stack.pop()
            n = self.count(w, min_mag, updated_after)
            if n == 0:
                continue
            if n <= limit or (w.end - w.start) <= MIN_WINDOW:
                if n > MAX_EVENTS_PER_QUERY:
                    log.warning("Window %s has %d events but can't be split further", w, n)
                yield w, n
            else:
                left, right = w.split()
                stack.extend([right, left])  # pop left first -> chronological order


def write_raw(df: pd.DataFrame, out_dir: Path, run_id: str, window: Window) -> Path:
    """Write one window's events to data/raw/usgs/run_id=<run>/<start>_<end>.parquet."""
    part_dir = out_dir / f"run_id={run_id}"
    part_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{window.start:%Y%m%dT%H%M%S}_{window.end:%Y%m%dT%H%M%S}.parquet"
    path = part_dir / fname
    df = df.assign(_source=SOURCE, _ingested_at=run_id)
    df.to_parquet(path, index=False)
    return path


def load_state(state_path: Path) -> dict:
    if state_path.exists():
        return json.loads(state_path.read_text())
    return {}


def save_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(state_path)  # atomic, so a crash never leaves a half-written state file


def run(
    window: Window,
    min_mag: float,
    out_dir: Path,
    state_path: Path,
    updated_after: datetime | None = None,
    client: USGSClient | None = None,
    restart: bool = False,
) -> dict:
    """Pull every event in `window` and record the high-water mark for the next run."""
    client = client or USGSClient()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    state = load_state(state_path)
    watermark = _parse_dt(state["max_updated"]) if "max_updated" in state else None
    is_backfill = updated_after is None

    # Resume an interrupted backfill: pieces are pulled in time order, so
    # everything before the saved cursor is already on disk.
    if is_backfill and restart:
        state.pop("backfill_done_until", None)
    # Only resume if this is the same backfill (same start date) as last time;
    # a backfill with a different start is a new job and pulls its whole range.
    if is_backfill and state.get("backfill_start") != _iso(window.start):
        state.pop("backfill_done_until", None)
        state["backfill_start"] = _iso(window.start)
    if is_backfill and "backfill_done_until" in state:
        cursor = _parse_dt(state["backfill_done_until"])
        if window.start < cursor < window.end:
            log.info("Resuming backfill from %s", _iso(cursor))
            window = Window(cursor, window.end)
        elif cursor >= window.end:
            log.info("Backfill already complete up to %s; nothing to do", _iso(cursor))
            return state

    total, files = 0, 0
    for piece, expected in client.plan_windows(window, min_mag, updated_after):
        df = client.query(piece, min_mag, updated_after)
        if not df.empty:
            if len(df) < expected:
                log.warning("Window %s: expected %d events, got %d", piece, expected, len(df))
            write_raw(df, out_dir, run_id, piece)
            total += len(df)
            files += 1

            # Watermark comes from USGS's own `updated` field, not our clock,
            # so clock differences between machines can't make us skip events.
            piece_max = max(_parse_dt(v) for v in df["updated"] if v)
            watermark = max(watermark, piece_max) if watermark else piece_max
            state |= {"max_updated": _iso(watermark), "min_mag": min_mag}
            log.info("%s  %6d events", piece, len(df))

        # Save progress after every piece so an interrupted run can resume.
        if is_backfill:
            state["backfill_done_until"] = _iso(piece.end)
        save_state(state_path, state)

    if is_backfill:
        state["backfill_done_until"] = _iso(window.end)
    state |= {"last_run_id": run_id, "last_run_events": total}
    save_state(state_path, state)
    log.info("Done: %d events in %d files (run_id=%s)", total, files, run_id)
    return state


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ingest USGS earthquake events into the raw layer.")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--backfill", action="store_true", help="pull all events from --start to --end")
    mode.add_argument("--incremental", action="store_true", help="pull events new/updated since last run")
    p.add_argument("--start", default="1900-01-01", help="backfill start date (UTC)")
    p.add_argument("--end", default=None, help="backfill end date (UTC, default: now)")
    p.add_argument("--min-mag", type=float, default=DEFAULT_MIN_MAG)
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                   help="incremental: how far back to look for revised events")
    p.add_argument("--restart", action="store_true",
                   help="backfill: ignore saved progress and pull the whole range again")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    out_dir = args.data_dir / "raw" / SOURCE
    state_path = args.data_dir / "state" / f"{SOURCE}.json"
    now = datetime.now(timezone.utc)

    if args.backfill:
        end = _parse_dt(args.end) if args.end else now
        window = Window(_parse_dt(args.start), end)
        run(window, args.min_mag, out_dir, state_path, restart=args.restart)
    else:
        state = load_state(state_path)
        if "max_updated" not in state:
            p.error("no previous run found -- run --backfill first")
        updated_after = _parse_dt(state["max_updated"])
        window = Window(now - timedelta(days=args.lookback_days), now)
        run(window, args.min_mag, out_dir, state_path, updated_after=updated_after)


if __name__ == "__main__":
    main()
