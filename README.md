# quake-pipeline

An earthquake data pipeline that pulls live data from the USGS (and soon JMA), stores it in layered Parquet files, and feeds a severity-prediction model.

This is the rebuilt version of my [Earthquake Severity Prediction Project](https://github.com/grbohanec/Earthquake-Severity-Prediction-Project) (v1). That was a one-time Spark notebook run on 51 hand-downloaded CSVs; this one ingests data automatically and keeps itself up to date.

## Status

- [x] **USGS ingestion**: backfill and incremental pulls, M2.0+
- [x] **Cleaned layer**: typed, de-duplicated, validated, partitioned by year
- [x] **Daily refresh**: GitHub Actions pulls new quakes every morning and syncs to S3 (needs AWS setup, see below)
- [ ] Data quality checks
- [ ] JMA ingestion (small quakes in Japan)
- [ ] Model rebuild
- [ ] Dashboard

## Setup

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## Ingesting USGS data

**First run (backfill).** Pulls every M2.0+ earthquake in a date range. Start small to check it works:

```bash
quake-ingest-usgs --backfill --start 2026-09-01
```

Then the full history. It takes a while; if it's interrupted, run the same command again and it continues where it stopped:

```bash
quake-ingest-usgs --backfill --start 1880-01-01
```

**After that (incremental).** Pulls only events that are new or were revised since the last run:

```bash
quake-ingest-usgs --incremental
```

Options: `--min-mag` (default 2.0), `--lookback-days` (default 30), `--data-dir` (default `data/`), `--restart` (ignore saved backfill progress and start over).

### How it works

- **The 20,000-event limit.** USGS returns at most 20,000 events per request. Before downloading, the script asks USGS how many events a date range holds and keeps halving the range until each piece fits. Long backfills are first cut into one-year chunks, and if USGS times out on a chunk, that chunk is split again.
- **Raw layer.** Each piece is saved as-is to `data/raw/usgs/run_id=<timestamp>/`, with every column kept as text. Nothing is cleaned or dropped here, so the raw layer is always a faithful copy of what USGS sent.
- **Incremental pulls.** USGS revises events after they happen (magnitudes get refined, locations corrected). The script tracks the latest `updated` timestamp it has seen in `data/state/usgs.json` and next time asks only for events updated after that. Revisions are resolved in the cleaned layer by keeping the newest version of each event `id`.
- **Reliability.** Rate limits and server errors are retried with backoff, and progress is saved after every piece, so an interrupted backfill picks up from where it stopped instead of starting over.

## Building the cleaned layer

```bash
quake-clean
```

Reads every raw file and writes one tidy dataset to `data/clean/usgs/year=YYYY/`:

- **Types.** Text becomes proper timestamps and numbers; blank values become nulls, not zeros.
- **Clear column names.** `magType` becomes `mag_type`, `depth` becomes `depth_km`, and so on.
- **One row per earthquake.** Overlapping downloads and USGS revisions are collapsed, keeping the newest version of each event.
- **Sanity checks.** Rows missing a time, location or magnitude, or with impossible values, are dropped, and the run reports how many were dropped and why.
- **Safe rebuilds.** The layer is built in a temporary folder and swapped in only when complete.

Built with DuckDB, which streams through the files rather than loading everything into memory. The original v1 dataset (1M rows) cleans in a few seconds.

## Daily refresh (GitHub Actions + S3)

`.github/workflows/daily-refresh.yml` runs every day at 06:17 Tokyo time (and on demand from the Actions tab):

1. Downloads `data/raw` and `data/state` from S3.
2. Runs `quake-ingest-usgs --incremental` to pull new and revised events.
3. Rebuilds the cleaned layer with `quake-clean`.
4. Syncs raw, state and clean back to S3.

The bucket and region default to `gabe-quake-pipeline` and `us-east-2` (override them with repository variables `S3_BUCKET` and `AWS_REGION`). It needs one way to authenticate:

- **GitHub OIDC (preferred).** Set the variable `AWS_ROLE_ARN` to an IAM role that trusts GitHub's OIDC provider. GitHub issues a short-lived token, so no AWS keys are stored anywhere.
- **Access key (fallback).** Set the secrets `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for an IAM user whose only permission is reading and writing `s3://<bucket>/data/*`. This project currently uses this, because its AWS account type blocks creating OIDC providers.

## Tests

```bash
pytest
```

The tests use a fake USGS API, so they run offline.

## Layout

```
src/quake_pipeline/
  ingest/usgs.py      USGS ingestion  (source -> raw)
  transform/clean.py  cleaning        (raw -> clean)
tests/                unit tests
data/                 (git-ignored) raw/, clean/, state/
```
