"""Data quality checks on the clean layer, run before it is published.

The daily job follows a write-audit-publish pattern:

    write    the clean step builds the clean layer locally (not yet visible to Athena)
    audit    quake-dq runs the checks below against that build
    publish  only if every error-level check passes is the build synced to S3

So a bad build (an upstream schema change, a stalled feed, a bug in the cleaning
SQL) fails the run loudly and Athena keeps serving yesterday's good data.

Each source is its own dataset with its own audit and publish step
(`--dataset usgs` or `--dataset jma`), so a JMA outage never stops the USGS data
from being published, and the reverse.

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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quake_pipeline.transform import clean_jma, match
from quake_pipeline.transform.clean import COLUMNS, SUMMARY_FILE

log = logging.getLogger(__name__)

ERROR, WARN = "error", "warn"

# USGS defaults, all overridable on the command line.
FRESHNESS_HOURS = 48  # M2.0+ quakes happen worldwide many times a day; 2 days with none = stalled feed
MAX_VOLUME_DROP = 0.01  # the history only grows; losing >1% of rows since the last good run is a bug
MAX_DROP_RATE = 0.01  # cleaning normally rejects ~0% of rows; more suggests a source schema change

KNOWN_ISSUES_FILE = Path("quality/known_issues.csv")

_DUCKDB_TYPES = {"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE"}
SHINDO_SQL = "(" + ", ".join(f"'{v}'" for v in clean_jma.SHINDO) + ")"


@dataclass
class Thresholds:
    freshness_hours: float = FRESHNESS_HOURS
    max_volume_drop: float = MAX_VOLUME_DROP
    max_drop_rate: float = MAX_DROP_RATE


@dataclass(frozen=True)
class Dataset:
    """What to check for one source's clean layer."""

    name: str
    clean_dir: str  # under data/clean/
    schema: dict  # column -> DuckDB type name
    required: list  # columns that must never be null
    # (name, severity, SQL condition marking a row BAD, expected). Only these can have
    # known issues acknowledged, because they are about single records.
    row_checks: list
    summary_file: str  # written by the clean step, under data/state/
    baseline_file: str  # last good run's row count, under data/state/
    thresholds: Thresholds = field(default_factory=Thresholds)


USGS = Dataset(
    name="usgs",
    clean_dir="usgs",
    schema={clean: _DUCKDB_TYPES.get(t, t) for clean, t in COLUMNS.values()}
    | {"source": "VARCHAR", "ingested_at": "VARCHAR", "year": "BIGINT"},
    required=["event_id", "event_time", "latitude", "longitude", "mag"],
    row_checks=[
        ("latitude in range", ERROR, "latitude NOT BETWEEN -90 AND 90", "-90..90"),
        ("longitude in range", ERROR, "longitude NOT BETWEEN -180 AND 180", "-180..180"),
        ("magnitude in range", ERROR, "mag NOT BETWEEN -2 AND 10", "-2..10"),
        ("depth plausible", WARN, "depth_km NOT BETWEEN -10 AND 800", "-10..800 km"),
        ("year partition matches event_time", ERROR, "year <> year(event_time)", "0 mismatches"),
        ("updated_at not before event_time", WARN, "updated_at < event_time", "0 rows"),
    ],
    summary_file=SUMMARY_FILE,
    baseline_file="quality.json",
)

JMA = Dataset(
    name="jma",
    clean_dir="jma",
    schema={c: _DUCKDB_TYPES.get(t, t) for c, t in clean_jma.COLUMNS.items()} | {"year": "BIGINT"},
    # Magnitude may be missing: JMA locates quakes too small to measure, and they are kept.
    required=["event_id", "event_time", "latitude", "longitude", "status"],
    row_checks=[
        ("latitude in range", ERROR, "latitude NOT BETWEEN -90 AND 90", "-90..90"),
        ("longitude in range", ERROR, "longitude NOT BETWEEN -180 AND 180", "-180..180"),
        ("magnitude in range", ERROR, "mag NOT BETWEEN -3 AND 10", "-3..10 or none"),
        ("depth plausible", WARN, "depth_km NOT BETWEEN -5 AND 800", "-5..800 km"),
        ("year partition matches event_time", ERROR, "year <> year(event_time)", "0 mismatches"),
        ("status is final or preliminary", ERROR, "status NOT IN ('final', 'preliminary')", "0 rows"),
        ("shindo is a JMA level", ERROR, f"max_intensity NOT IN {SHINDO_SQL}", "1..7 or none"),
    ],
    summary_file=clean_jma.SUMMARY_FILE,
    baseline_file="quality_jma.json",
    thresholds=Thresholds(
        # Daily lists run 2 days behind, and the run is early in the Japan morning.
        freshness_hours=96,
        # When a new catalog year replaces that year's preliminary rows, the count
        # can legitimately move by a few percent.
        max_volume_drop=0.05,
    ),
)

DATASETS = {d.name: d for d in (USGS, JMA)}

# Kept for backwards compatibility with code that imported these names.
EXPECTED_SCHEMA = USGS.schema
REQUIRED_COLUMNS = USGS.required
ROW_CHECKS = USGS.row_checks

FELT_FRESH_HOURS = 72  # Japan has felt earthquakes most days; three quiet days in a row is unusual
MIN_M5_MATCH_RATE = 0.90  # nearly every M5+ quake in Japan should also be in the USGS catalog


def load_known_issues(path: Path | None) -> dict[str, set[str]]:
    """Read acknowledged records: check name -> event ids. Fails fast on typos."""
    if path is None or not path.exists():
        return {}
    valid = {name for d in DATASETS.values() for name, *_ in d.row_checks}
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


def _scalar(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def _read_json(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def run_checks(
    data_dir: Path,
    now: datetime,
    t: Thresholds | None = None,
    known: dict[str, set[str]] | None = None,
    dataset: Dataset = USGS,
) -> list[CheckResult]:
    t = t or dataset.thresholds
    known = known or {}
    clean_dir = data_dir / "clean" / dataset.clean_dir
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
    expected_schema = dataset.schema
    actual = {name: typ for name, typ, *_ in con.execute("DESCRIBE q").fetchall()}
    missing = sorted(set(expected_schema) - set(actual))
    wrong = sorted(c for c in expected_schema if c in actual and actual[c] != expected_schema[c])
    extra = sorted(set(actual) - set(expected_schema))
    problems = [f"missing {missing}"] * bool(missing) + [f"wrong type {wrong}"] * bool(wrong)
    add("schema matches", ERROR, not problems, "; ".join(problems) or "ok", f"{len(expected_schema)} columns")
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
        "SELECT " + ", ".join(f"count(*) FILTER (WHERE {c} IS NULL)" for c in dataset.required) + " FROM q"
    ).fetchone()
    null_report = {c: n for c, n in zip(dataset.required, nulls, strict=True) if n}
    add("required fields present", ERROR, not null_report, null_report or "no nulls", "no nulls", sum(nulls))

    # --- Validity: values a real earthquake can have.
    # Acknowledged records (known issues) are excluded, so only new problems count.
    stale: list[str] = []
    for check, severity, bad, expected in dataset.row_checks:
        failing = {r[0] for r in con.execute(f"SELECT event_id FROM q WHERE {bad}").fetchall()}
        # The known-issues file covers every dataset; JMA ids all start with "jma-".
        acked = {e for e in known.get(check, set()) if e.startswith("jma-") == (dataset is JMA)}
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

    summary = _read_json(data_dir / "state" / dataset.summary_file)
    if dataset is JMA:
        _jma_checks(con, data_dir, now, summary, add)
    con.close()

    # --- Volume: compared with the last run that passed.
    baseline = _read_baseline(data_dir, dataset)
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
        add("cleaning rejected few rows", WARN, False, f"no {dataset.summary_file}", "build summary present")

    return results


def _jma_checks(
    con: duckdb.DuckDBPyConnection, data_dir: Path, now: datetime, summary: dict | None, add
) -> None:
    """Checks only JMA needs: its three sources must fit together without gaps."""
    # --- No missing days: Japan has located earthquakes every day, so a day with
    # none means a daily list failed to download or parse. Counted from the end of
    # the catalog, so a gap *between* the catalog and the daily lists is caught too.
    first_day = None
    if summary and summary.get("catalog_covers_until", "0000") != "00000101":
        first_day = datetime.strptime(summary["catalog_covers_until"], "%Y%m%d").date()
    days = con.execute("""
        SELECT DISTINCT CAST(event_time + INTERVAL 9 HOUR AS DATE) AS d
        FROM q WHERE status = 'preliminary' ORDER BY d
    """).fetchall()
    days = [d for (d,) in days]
    if days:
        start = first_day or days[0]
        expected = {start + timedelta(days=i) for i in range((days[-1] - start).days + 1)}
        gaps = sorted(expected - set(days))
        shown = ", ".join(str(d) for d in gaps[:5]) + (" ..." if len(gaps) > 5 else "")
        add(
            "no missing days in daily lists",
            ERROR,
            not gaps,
            f"{len(gaps)} missing ({shown})" if gaps else f"{len(days)} days, {start} to {days[-1]}",
            "0 missing (JST)",
            len(gaps),
        )

    # --- Felt-earthquake archive (built from feed snapshots).
    felt_dir = data_dir / "clean" / "jma_felt"
    if any(felt_dir.glob("year=*/*.parquet")):
        glob = (felt_dir / "**" / "*.parquet").as_posix()
        con.execute(f"CREATE VIEW felt AS SELECT * FROM read_parquet('{glob}', hive_partitioning = true)")
        n, dupes, bad, newest = con.execute(f"""
            SELECT count(*), count(*) - count(DISTINCT report_id),
                   count(*) FILTER (WHERE max_intensity NOT IN {SHINDO_SQL}), epoch(max(event_time))
            FROM felt
        """).fetchone()
        add("felt reports unique", ERROR, dupes == 0, f"{dupes:,} duplicates of {n:,}", "0", dupes)
        add("felt shindo is a JMA level", ERROR, bad == 0, f"{bad:,} rows", "1..7", bad)
        age_h = (now.timestamp() - newest) / 3600
        add(
            "felt feed is fresh",
            WARN,
            age_h <= FELT_FRESH_HOURS,
            f"newest felt quake {age_h:.1f}h old",
            f"<= {FELT_FRESH_HOURS}h",
            round(age_h, 2),
        )
    else:
        add("felt feed is fresh", WARN, False, "no felt-earthquake data", "feed snapshots present")

    # --- Cross-source reconciliation: two agencies should agree on the big ones.
    # Nearly every M5+ JMA earthquake around Japan should have a USGS twin; if not,
    # the matching (or one of the sources) has gone wrong.
    matches = data_dir / "clean" / match.OUT_NAME / "matches.parquet"
    if matches.exists():
        (lat0, lat1), (lon0, lon1) = match.REGION["lat"], match.REGION["lon"]
        total, matched = con.execute(f"""
            SELECT count(*), count(m.jma_event_id)
            FROM q LEFT JOIN read_parquet('{matches.as_posix()}') m ON m.jma_event_id = q.event_id
            WHERE q.mag >= 5 AND q.event_type = 'earthquake' AND q.event_time >= TIMESTAMPTZ '2000-01-01'
              AND q.latitude BETWEEN {lat0} AND {lat1} AND q.longitude BETWEEN {lon0} AND {lon1}
        """).fetchone()
        if total:
            rate = matched / total
            add(
                "M5+ quakes also in USGS",
                WARN,
                rate >= MIN_M5_MATCH_RATE,
                f"{matched:,} of {total:,} ({rate:.1%})",
                f">= {MIN_M5_MATCH_RATE:.0%}",
                rate,
            )


def _read_baseline(data_dir: Path, dataset: Dataset) -> dict | None:
    return _read_json(data_dir / "state" / dataset.baseline_file)


def write_results(
    results: list[CheckResult],
    data_dir: Path,
    run_at: datetime,
    dataset: str = "usgs",
    run_id: str | None = None,
) -> Path:
    """Append this run's results to the dq history (one small Parquet file per dataset per run)."""
    run_id = run_id or run_at.strftime("%Y%m%dT%H%M%SZ")
    out = data_dir / "dq" / "results" / f"dq_{run_id}_{dataset}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [{"run_id": run_id, "run_at": run_at, **asdict(r), "dataset": dataset} for r in results],
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
                ("dataset", pa.string()),
            ]
        ),
    )
    pq.write_table(table, out)
    return out


def update_baseline(
    data_dir: Path, results: list[CheckResult], run_at: datetime, dataset: Dataset = USGS
) -> None:
    rows = next(r.value for r in results if r.check == "not empty")
    path = data_dir / "state" / dataset.baseline_file
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({"row_count": int(rows), "run_at": run_at.isoformat(timespec="seconds")}, indent=2)
    )
    tmp.replace(path)


def to_markdown(results: list[CheckResult], dataset: str = "usgs") -> str:
    failed = [r for r in results if r.blocking]
    status = "FAILED, clean layer not published" if failed else "passed"
    head = f"### Data quality ({dataset.upper()}): {status}"
    lines = [head, "", "| | Check | Severity | Observed | Expected |", "| --- | --- | --- | --- | --- |"]
    for r in results:
        icon = "✅" if r.passed else ("❌" if r.severity == ERROR else "⚠️")
        lines.append(f"| {icon} | {r.check} | {r.severity} | {r.observed} | {r.expected} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run data quality checks on a clean layer before publishing.")
    p.add_argument("--dataset", choices=sorted(DATASETS), default="usgs")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    p.add_argument("--freshness-hours", type=float, default=None, help="default depends on the dataset")
    p.add_argument("--max-volume-drop", type=float, default=None, help="default depends on the dataset")
    p.add_argument("--max-drop-rate", type=float, default=None, help="default depends on the dataset")
    p.add_argument(
        "--known-issues",
        type=Path,
        default=KNOWN_ISSUES_FILE,
        help="CSV of acknowledged records (check, event_id, reason, acknowledged_on)",
    )
    p.add_argument(
        "--run-id",
        default=os.environ.get("QUAKE_RUN_ID"),
        help="groups the datasets checked in one pipeline run (default: $QUAKE_RUN_ID, else the time now)",
    )
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    dataset = DATASETS[args.dataset]
    now = datetime.now(timezone.utc)
    d = dataset.thresholds
    t = Thresholds(
        d.freshness_hours if args.freshness_hours is None else args.freshness_hours,
        d.max_volume_drop if args.max_volume_drop is None else args.max_volume_drop,
        d.max_drop_rate if args.max_drop_rate is None else args.max_drop_rate,
    )
    known = load_known_issues(args.known_issues)
    if known:
        log.info("Known issues acknowledged: %d", sum(map(len, known.values())))
    results = run_checks(args.data_dir, now, t, known, dataset)

    for r in results:
        status = "PASS" if r.passed else ("FAIL" if r.severity == ERROR else "WARN")
        log.info("%-4s %-36s %s (expected %s)", status, r.check, r.observed, r.expected)

    out = write_results(results, args.data_dir, now, dataset.name, args.run_id)
    log.info("Results written to %s", out)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(to_markdown(results, dataset.name))

    failed = [r.check for r in results if r.blocking]
    if failed:
        log.error("%d check(s) failed: %s", len(failed), ", ".join(failed))
        return 1
    update_baseline(args.data_dir, results, now, dataset)
    return 0


def cli() -> None:
    sys.exit(main())


if __name__ == "__main__":
    cli()
