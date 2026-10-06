"""Data quality checks on the clean layer, run before it is published.

The daily job follows a write-audit-publish pattern:

    write    quake-clean builds the clean layer locally (not yet visible to Athena)
    audit    quake-dq runs the checks below against that build
    publish  only if every error-level check passes is the build synced to S3

So a bad build (an upstream schema change, a stalled feed, a bug in the cleaning
SQL) fails the run loudly and Athena keeps serving yesterday's good data.

Known issues: some source records are odd but correct to keep (e.g. a USGS record
with an impossible timestamp). Once a person has reviewed one, it is listed in
quality/known_issues.csv with a reason, and the row-level checks skip it, so the
check only fires on *new* problems. A listed record that no longer fails is
reported, so the list never silently goes stale.

Each check has a severity:

* error -- the build is wrong or untrustworthy; the run fails and nothing is published.
* warn  -- worth a look, but the data is still usable; reported, never blocks.

Every run's results are also written to data/dq/results/ as Parquet, so the
history of checks is queryable in Athena (table quakes.dq_results).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quake_pipeline.transform.clean import COLUMNS, SUMMARY_FILE

log = logging.getLogger(__name__)

ERROR, WARN = "error", "warn"

# Defaults, all overridable on the command line.
FRESHNESS_HOURS = 48  # M2.0+ quakes happen worldwide many times a day; 2 days with none = stalled feed
MAX_VOLUME_DROP = 0.01  # the history only grows; losing >1% of rows since the last good run is a bug
MAX_DROP_RATE = 0.01  # cleaning normally rejects ~0% of rows; more suggests a source schema change

BASELINE_FILE = "quality.json"  # last good run's row count, kept with the ingest state

# Expected clean-layer schema (DuckDB type names as read back from Parquet).
_DUCKDB_TYPES = {"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE"}
EXPECTED_SCHEMA = {clean: _DUCKDB_TYPES.get(t, t) for clean, t in COLUMNS.values()} | {
    "source": "VARCHAR",
    "ingested_at": "VARCHAR",
    "year": "BIGINT",
}

REQUIRED_COLUMNS = ["event_id", "event_time", "latitude", "longitude", "mag"]

KNOWN_ISSUES_FILE = Path("quality/known_issues.csv")

# Row-level checks: (name, severity, SQL condition that marks a row as BAD, expected).
# Only these can have known issues acknowledged, because they are about single records.
ROW_CHECKS = [
    ("latitude in range", ERROR, "latitude NOT BETWEEN -90 AND 90", "-90..90"),
    ("longitude in range", ERROR, "longitude NOT BETWEEN -180 AND 180", "-180..180"),
    ("magnitude in range", ERROR, "mag NOT BETWEEN -2 AND 10", "-2..10"),
    ("depth plausible", WARN, "depth_km NOT BETWEEN -10 AND 800", "-10..800 km"),
    ("year partition matches event_time", ERROR, "year <> year(event_time)", "0 mismatches"),
    ("updated_at not before event_time", WARN, "updated_at < event_time", "0 rows"),
]


def load_known_issues(path: Path | None) -> dict[str, set[str]]:
    """Read acknowledged records: check name -> event ids. Fails fast on typos."""
    if path is None or not path.exists():
        return {}
    valid = {name for name, *_ in ROW_CHECKS}
    known: dict[str, set[str]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for line, row in enumerate(csv.DictReader(f), start=2):
            check, event_id = (row.get("check") or "").strip(), (row.get("event_id") or "").strip()
            if check not in valid:
                raise ValueError(f"{path}:{line}: unknown check {check!r}; must be one of {sorted(valid)}")
            if not event_id or not (row.get("reason") or "").strip():
                raise ValueError(f"{path}:{line}: event_id and reason are required")
            known.setdefault(check, set()).add(event_id)
    return known


@dataclass
class CheckResult:
    check: str
    severity: str
    passed: bool
    observed: str
    expected: str
    value: float | None = None  # the measured number, so metrics can be charted over time

    @property
    def blocking(self) -> bool:
        return self.severity == ERROR and not self.passed


@dataclass
class Thresholds:
    freshness_hours: float = FRESHNESS_HOURS
    max_volume_drop: float = MAX_VOLUME_DROP
    max_drop_rate: float = MAX_DROP_RATE


def _scalar(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def _read_json(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def run_checks(
    data_dir: Path, now: datetime, t: Thresholds, known: dict[str, set[str]] | None = None
) -> list[CheckResult]:
    known = known or {}
    clean_dir = data_dir / "clean" / "usgs"
    if not any(clean_dir.glob("year=*/*.parquet")):
        return [CheckResult("clean layer exists", ERROR, False, "no Parquet files", f"files in {clean_dir}")]

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    glob = (clean_dir / "**" / "*.parquet").as_posix()
    con.execute(f"CREATE VIEW q AS SELECT * FROM read_parquet('{glob}', hive_partitioning = true)")
    results: list[CheckResult] = []

    def add(check: str, severity: str, passed: bool, observed, expected, value=None) -> None:
        v = None if value is None else float(value)
        results.append(CheckResult(check, severity, bool(passed), str(observed), str(expected), v))

    # --- Schema: catches an upstream rename or a type drifting in the cleaning SQL.
    actual = {name: typ for name, typ, *_ in con.execute("DESCRIBE q").fetchall()}
    missing = sorted(set(EXPECTED_SCHEMA) - set(actual))
    wrong = sorted(c for c in EXPECTED_SCHEMA if c in actual and actual[c] != EXPECTED_SCHEMA[c])
    extra = sorted(set(actual) - set(EXPECTED_SCHEMA))
    problems = [f"missing {missing}"] * bool(missing) + [f"wrong type {wrong}"] * bool(wrong)
    add("schema matches", ERROR, not problems, "; ".join(problems) or "ok", f"{len(EXPECTED_SCHEMA)} columns")
    if extra:  # new columns don't break readers, but Athena won't see them until the DDL is updated
        add("no unexpected columns", WARN, False, extra, "none")
    if missing or wrong:
        return results  # later checks assume the schema

    rows = _scalar(con, "SELECT count(*) FROM q")
    add("not empty", ERROR, rows > 0, f"{rows:,} rows", "> 0", rows)

    # --- Completeness and uniqueness.
    dupes = _scalar(con, "SELECT count(*) - count(DISTINCT event_id) FROM q")
    add("event_id unique", ERROR, dupes == 0, f"{dupes:,} duplicates", "0", dupes)

    nulls = con.execute(
        "SELECT " + ", ".join(f"count(*) FILTER (WHERE {c} IS NULL)" for c in REQUIRED_COLUMNS) + " FROM q"
    ).fetchone()
    null_report = {c: n for c, n in zip(REQUIRED_COLUMNS, nulls, strict=True) if n}
    add("required fields present", ERROR, not null_report, null_report or "no nulls", "no nulls", sum(nulls))

    # --- Validity: values a real earthquake can have.
    # Acknowledged records (known issues) are excluded, so only new problems count.
    stale: list[str] = []
    for check, severity, bad, expected in ROW_CHECKS:
        failing = {r[0] for r in con.execute(f"SELECT event_id FROM q WHERE {bad}").fetchall()}
        acked = known.get(check, set())
        new = failing - acked
        stale += [f"{e} ({check})" for e in sorted(acked - failing)]
        note = f" ({len(failing & acked)} known, acknowledged)" if failing & acked else ""
        add(check, severity, not new, f"{len(new):,} rows{note}", expected, len(new))
    if stale:  # the record was fixed upstream or removed: drop it from known_issues.csv
        add(
            "known issues still apply",
            WARN,
            False,
            f"no longer failing: {', '.join(stale)}",
            "none stale",
            len(stale),
        )

    future_cutoff = now + timedelta(hours=1)  # small allowance for clock skew
    n_future = _scalar(
        con, f"SELECT count(*) FROM q WHERE event_time > TIMESTAMPTZ '{future_cutoff.isoformat()}'"
    )
    add("no events in the future", ERROR, n_future == 0, f"{n_future:,} rows", "0", n_future)

    # --- Freshness: the feed is still flowing.
    latest = _scalar(con, "SELECT epoch(max(event_time)) FROM q")  # seconds since 1970, UTC
    age_h = (now.timestamp() - latest) / 3600 if latest is not None else float("inf")
    add(
        "data is fresh",
        ERROR,
        age_h <= t.freshness_hours,
        f"newest event {age_h:.1f}h old",
        f"<= {t.freshness_hours:g}h",
        round(age_h, 2) if latest is not None else None,
    )
    con.close()

    # --- Volume: compared with the last run that passed.
    baseline = _read_json(data_dir / "state" / BASELINE_FILE)
    if baseline:
        prev = baseline["row_count"]
        change = (rows - prev) / prev if prev else 0.0
        add(
            "row count did not drop",
            ERROR,
            change >= -t.max_volume_drop,
            f"{rows:,} vs {prev:,} last good run ({change:+.2%})",
            f">= -{t.max_volume_drop:.0%}",
            change,
        )
    else:
        add("row count did not drop", WARN, True, f"{rows:,} rows (first run, baseline set)", "baseline")

    # --- Rejection rate: how much the cleaning step threw away.
    summary = _read_json(data_dir / "state" / SUMMARY_FILE)
    if summary:
        unique = summary["raw_rows"] - summary["duplicates_removed"]
        dropped = sum(summary["dropped"].values())
        rate = dropped / unique if unique else 0.0
        add(
            "cleaning rejected few rows",
            ERROR,
            rate <= t.max_drop_rate,
            f"{dropped:,} of {unique:,} ({rate:.3%})",
            f"<= {t.max_drop_rate:.0%}",
            rate,
        )
    else:
        add("cleaning rejected few rows", WARN, False, f"no {SUMMARY_FILE}", "build summary present")

    return results


def write_results(results: list[CheckResult], data_dir: Path, run_at: datetime) -> Path:
    """Append this run's results to the dq history (one small Parquet file per run)."""
    run_id = run_at.strftime("%Y%m%dT%H%M%SZ")
    out = data_dir / "dq" / "results" / f"dq_{run_id}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [{"run_id": run_id, "run_at": run_at, **asdict(r)} for r in results],
        schema=pa.schema(
            [
                ("run_id", pa.string()),
                ("run_at", pa.timestamp("us", tz="UTC")),
                ("check", pa.string()),
                ("severity", pa.string()),
                ("passed", pa.bool_()),
                ("observed", pa.string()),
                ("expected", pa.string()),
                ("value", pa.float64()),
            ]
        ),
    )
    pq.write_table(table, out)
    return out


def update_baseline(data_dir: Path, results: list[CheckResult], run_at: datetime) -> None:
    rows = next(r.value for r in results if r.check == "not empty")
    path = data_dir / "state" / BASELINE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({"row_count": int(rows), "run_at": run_at.isoformat(timespec="seconds")}, indent=2)
    )
    tmp.replace(path)


def to_markdown(results: list[CheckResult]) -> str:
    failed = [r for r in results if r.blocking]
    head = "### Data quality: " + ("FAILED, clean layer not published" if failed else "passed")
    lines = [head, "", "| | Check | Severity | Observed | Expected |", "| --- | --- | --- | --- | --- |"]
    for r in results:
        icon = "✅" if r.passed else ("❌" if r.severity == ERROR else "⚠️")
        lines.append(f"| {icon} | {r.check} | {r.severity} | {r.observed} | {r.expected} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run data quality checks on the clean layer before publishing.")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    p.add_argument("--freshness-hours", type=float, default=FRESHNESS_HOURS)
    p.add_argument("--max-volume-drop", type=float, default=MAX_VOLUME_DROP)
    p.add_argument("--max-drop-rate", type=float, default=MAX_DROP_RATE)
    p.add_argument(
        "--known-issues",
        type=Path,
        default=KNOWN_ISSUES_FILE,
        help="CSV of acknowledged records (check, event_id, reason, acknowledged_on)",
    )
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    now = datetime.now(timezone.utc)
    t = Thresholds(args.freshness_hours, args.max_volume_drop, args.max_drop_rate)
    known = load_known_issues(args.known_issues)
    if known:
        log.info("Known issues acknowledged: %d", sum(map(len, known.values())))
    results = run_checks(args.data_dir, now, t, known)

    for r in results:
        status = "PASS" if r.passed else ("FAIL" if r.severity == ERROR else "WARN")
        log.info("%-4s %-36s %s (expected %s)", status, r.check, r.observed, r.expected)

    out = write_results(results, args.data_dir, now)
    log.info("Results written to %s", out)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(to_markdown(results))

    failed = [r.check for r in results if r.blocking]
    if failed:
        log.error("%d check(s) failed: %s", len(failed), ", ".join(failed))
        return 1
    update_baseline(args.data_dir, results, now)
    return 0


def cli() -> None:
    sys.exit(main())


if __name__ == "__main__":
    cli()
