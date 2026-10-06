# quake-pipeline

[![CI](https://github.com/grbohanec/quake-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/grbohanec/quake-pipeline/actions/workflows/ci.yml)
[![Daily refresh](https://github.com/grbohanec/quake-pipeline/actions/workflows/daily-refresh.yml/badge.svg)](https://github.com/grbohanec/quake-pipeline/actions/workflows/daily-refresh.yml)

An automated data pipeline that pulls every M2.0+ earthquake since 1880 from the [USGS Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/) (about 1.55 million events), cleans it into partitioned Parquet on S3, and refreshes itself every morning. Each day's build must pass 14 data quality checks before it is published, so a bad run never reaches the queryable data. The full history is queryable with SQL in Amazon Athena, for a few cents a month.

This is the rebuilt version of my [Earthquake Severity Prediction Project](https://github.com/grbohanec/Earthquake-Severity-Prediction-Project). That was a one-time Spark notebook on 51 hand-downloaded CSVs that stopped at June 2024; this one ingests automatically and stays current.

## Architecture

```mermaid
flowchart LR
    usgs[USGS Event API] -->|new + revised events| ingest

    subgraph gha [GitHub Actions, daily 06:17 JST]
        ingest[Ingest<br/>Python] --> clean[Clean<br/>DuckDB] --> dq{Quality<br/>checks}
    end

    subgraph s3 [S3 bucket]
        raw[(data/raw)]
        state[(data/state)]
        cleaned[(data/clean)]
    end

    ingest <-->|sync| raw
    ingest <-->|watermark| state
    dq -->|pass: publish| cleaned
    dq -.->|fail: alert, keep last good build| alert[Email / Slack]
    dq -->|results| dqres[(data/dq)]
    cleaned --> athena[Athena SQL]
    dqres --> athena
```

1. **Ingest** asks USGS only for events added or revised since the last run, and saves them untouched to the raw layer.
2. **Clean** rebuilds a typed, de-duplicated, validated dataset from raw, partitioned by year.
3. **Quality checks** audit the new build. Only a build that passes is published; a failure stops the run and sends an alert.
4. **Athena** queries the clean Parquet files in place, plus the history of every quality check.

## Example

Strongest earthquakes near Japan in the last 10 years:

```sql
SELECT event_time AT TIME ZONE 'Asia/Tokyo' AS time_jst, mag, depth_km, place
FROM quakes.usgs_events
WHERE year >= year(current_date) - 10
  AND latitude BETWEEN 24 AND 46
  AND longitude BETWEEN 122 AND 146
ORDER BY mag DESC
LIMIT 5;
```

A per-year count over the whole dataset scans about 1 MB, because Parquet lets Athena read only the columns a query uses. More queries are in [`sql/athena/example_queries.sql`](sql/athena/example_queries.sql).

## Quickstart

Requires Python 3.10 or newer.

```bash
git clone https://github.com/grbohanec/quake-pipeline
cd quake-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest
```

Pull a month of data, then build the clean layer:

```bash
quake-ingest-usgs --backfill --start 2026-09-01
quake-clean
quake-dq --freshness-hours 1e9   # skip the freshness check for an old sample
```

Data is written to `data/` (git-ignored).

## Usage

### Ingest

| Command | What it does |
| --- | --- |
| `quake-ingest-usgs --backfill --start 1880-01-01` | Pulls every event in a date range. Safe to interrupt: re-running resumes where it stopped. |
| `quake-ingest-usgs --incremental` | Pulls only events new or revised since the last run. |

Options: `--min-mag` (default 2.0), `--lookback-days` (default 30), `--data-dir` (default `data/`), `--restart` (ignore saved backfill progress).

### Clean

```bash
quake-clean
```

Reads every raw file and writes `data/clean/usgs/year=YYYY/`:

- **Types:** text becomes timestamps and numbers; blanks become nulls, not zeros.
- **One row per earthquake:** overlapping downloads and USGS revisions collapse to the newest version of each event.
- **Validation:** rows missing a time, location or magnitude, or with impossible values, are dropped and counted.
- **Safe rebuilds:** built in a temporary folder and swapped in only when complete.

### Data quality

```bash
quake-dq
```

Audits the clean layer and exits with code 1 if any error-level check fails, which stops the daily job before publishing. Results go to the log, the GitHub Actions run summary, and `data/dq/results/` as Parquet.

| Check | Severity | Catches |
| --- | --- | --- |
| Schema matches (names and types) | error | USGS renaming a field; a type drifting in the cleaning SQL |
| Not empty; `event_id` unique | error | a broken build; a bad de-duplication |
| Required fields present | error | null time, location or magnitude |
| Latitude, longitude, magnitude in range; year partition matches `event_time` | error | impossible values; rows in the wrong folder |
| No events in the future | error | time-zone or parsing bugs |
| Data is fresh (newest event < 48h old) | error | a stalled feed: M2.0+ quakes happen worldwide many times a day |
| Row count did not drop > 1% vs. the last good run | error | data silently lost (the history only grows) |
| Cleaning rejected < 1% of rows | error | an upstream change making most rows invalid |
| Depth plausible; `updated_at` not before `event_time` | warn | odd but possible values, reported only |

**Known issues.** Some source records are odd but genuine, such as a USGS record whose `updated_at` is earlier than its `event_time`. After review, a record is listed in [`quality/known_issues.csv`](quality/known_issues.csv) with the reason and date, and the row-level checks skip it, so a warning means something *new*. If a listed record stops failing (fixed upstream), a `known issues still apply` warning says to remove it, so the list never goes stale. Changes to the list go through a pull request like any code change.

Thresholds are options: `--freshness-hours`, `--max-volume-drop`, `--max-drop-rate`. The last good run's row count is kept in `data/state/quality.json` and only moves forward when a run passes.

## How it works

- **The 20,000-event limit.** USGS returns at most 20,000 events per request. The ingester asks the `/count` endpoint first and halves date ranges until each piece fits. Long backfills start as one-year chunks, and timeouts trigger more splitting.
- **Raw layer.** Each response is saved as-is under `data/raw/usgs/run_id=<timestamp>/`, every column kept as text, so cleaning can always be re-run from the original data.
- **Incremental loads.** USGS revises events after they happen. The latest `updated` timestamp seen is stored in `data/state/usgs.json` as a watermark, and the next run asks only for events updated after it.
- **Reliability.** Rate limits and server errors are retried with backoff. Progress is saved after every chunk and files are written atomically, so any run can be safely repeated.

## Deployment (AWS + GitHub Actions)

[`daily-refresh.yml`](.github/workflows/daily-refresh.yml) runs every day at 06:17 Tokyo time, and on demand from the Actions tab. It downloads `data/raw` and `data/state` from S3, runs an incremental ingest, rebuilds the clean layer, runs the quality checks, and publishes the clean layer to S3 only if they pass (write, audit, publish).

**Alerts:** GitHub emails the workflow owner when a scheduled run fails. For a chat alert too, add a Slack or Discord incoming-webhook URL as the secret `ALERT_WEBHOOK_URL`; the message says whether the quality checks failed or the pipeline errored, and links to the run.

**Authentication** uses one of:

- **GitHub OIDC (preferred):** set the repository variable `AWS_ROLE_ARN` to an IAM role that trusts GitHub's OIDC provider. No keys are stored.
- **Access key (current setup):** secrets `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for an IAM user whose only permission is `s3://<bucket>/data/*`. This project uses it because its AWS account type blocks creating OIDC providers.

The bucket and region default to `gabe-quake-pipeline` and `us-east-2`; override them with repository variables `S3_BUCKET` and `AWS_REGION`.

**Athena:** run [`sql/athena/create_tables.sql`](sql/athena/create_tables.sql) once. It creates `quakes.usgs_events`, which uses partition projection so new years appear automatically with no Glue crawler, `quakes.dq_results`, the history of quality checks (one row per check per run), and the view `quakes.dq_runs`, which pivots that history to one row per run.

## Design decisions

| Decision | Why |
| --- | --- |
| DuckDB, not Spark | ~1.5M rows (~100 MB) clean in seconds on one machine; a cluster would take longer to start than the job takes to run. |
| Raw + clean layers | Cleaning logic can change or be fixed and re-run without re-downloading 146 years of data. |
| Full rebuild of the clean layer | Seconds at this size and always correct. At scale: Iceberg or Delta Lake with `MERGE`. |
| Partition by year, not month | Early decades have few events; monthly folders would create thousands of tiny files. |
| GitHub Actions, not Airflow | One daily job doesn't justify an orchestrator. |
| Timestamps in UTC | Converted only for display, avoiding time-zone bugs. |
| Write, audit, publish | A bad build is caught before Athena sees it; readers keep the last good data instead of wrong data. |
| Checks in plain SQL, not Great Expectations | 14 checks over one table fit in one readable file with no extra framework. Great Expectations or dbt tests would pay off with many tables. |
| Check history stored long, viewed wide | One row per check means adding a check never changes the table's schema; the `dq_runs` view pivots it to one row per run for reading. |
| Error vs. warn severity | Only problems that make the data wrong block publishing; odd-but-possible values are reported, so alerts stay meaningful. |
| Known issues acknowledged in a reviewed file | Silencing a check entirely would hide new problems; acknowledging specific records keeps the check sharp, and the file is an audit trail of every exception and why. |
| Volume check against the last *good* run | Comparing with yesterday would let a slow leak of a few rows a day pass every check. |

## Project layout

```
src/quake_pipeline/
  ingest/usgs.py          USGS API -> raw layer
  transform/clean.py      raw layer -> clean layer
  quality/checks.py       quality checks that gate publishing
tests/                    unit tests, mirroring src/ (offline, fake USGS API)
sql/athena/               table definition and example queries
.github/workflows/
  ci.yml                  lint + tests on every push
  daily-refresh.yml       scheduled pipeline run (write -> audit -> publish)
```

## Roadmap

- [x] USGS ingestion: backfill and incremental, M2.0+
- [x] Clean layer: typed, de-duplicated, validated, partitioned
- [x] Daily refresh on GitHub Actions, stored in S3, queryable in Athena
- [x] Data quality checks and failure alerts
- [ ] JMA source for small earthquakes in Japan
- [ ] Severity model rebuilt on the clean layer
- [ ] Dashboard

## License

[MIT](LICENSE)
