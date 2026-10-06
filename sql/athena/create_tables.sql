-- Athena tables over the cleaned layer in S3.
-- Run once in the Athena query editor (one statement at a time).
--
-- Partition projection tells Athena how the year=YYYY folders are laid out,
-- so new years are picked up automatically: no Glue crawler, no MSCK REPAIR.

CREATE DATABASE IF NOT EXISTS quakes;

CREATE EXTERNAL TABLE IF NOT EXISTS quakes.usgs_events (
    event_id             string,
    event_time           timestamp,
    updated_at           timestamp,
    latitude             double,
    longitude            double,
    depth_km             double,
    mag                  double,
    mag_type             string,
    nst                  int,
    gap                  double,
    dmin                 double,
    rms                  double,
    network              string,
    place                string,
    event_type           string,
    horizontal_error_km  double,
    depth_error_km       double,
    mag_error            double,
    mag_nst              int,
    review_status        string,
    location_source      string,
    mag_source           string,
    source               string,
    ingested_at          string
)
PARTITIONED BY (year int)
STORED AS PARQUET
LOCATION 's3://gabe-quake-pipeline/data/clean/usgs/'
TBLPROPERTIES (
    'projection.enabled'        = 'true',
    'projection.year.type'      = 'integer',
    'projection.year.range'     = '1880,2100',
    'storage.location.template' = 's3://gabe-quake-pipeline/data/clean/usgs/year=${year}/'
);

-- History of data quality check results: one row per check per daily run,
-- written by quake-dq whether the run passed or failed.
CREATE EXTERNAL TABLE IF NOT EXISTS quakes.dq_results (
    run_id    string,
    run_at    timestamp,
    `check`   string,
    severity  string,   -- 'error' blocks publishing, 'warn' is report-only
    passed    boolean,
    observed  string,
    expected  string,
    value     double    -- the measured number (row count, hours since newest event, ...)
)
STORED AS PARQUET
LOCATION 's3://gabe-quake-pipeline/data/dq/results/';

-- One row per daily run, pivoted from dq_results for easy reading.
-- dq_results stays in long format (one row per check) so new checks never
-- change its schema; this view presents the same data wide.
CREATE OR REPLACE VIEW quakes.dq_runs AS
SELECT
    run_id,
    run_at,
    bool_and(passed OR severity = 'warn')                    AS published,
    count_if(NOT passed AND severity = 'error')              AS errors,
    count_if(NOT passed AND severity = 'warn')               AS warnings,
    max(CASE WHEN "check" = 'not empty'     THEN value END)  AS row_count,
    max(CASE WHEN "check" = 'data is fresh' THEN value END)  AS hours_since_newest_event,
    array_join(array_agg(CASE WHEN NOT passed THEN "check" END), ', ') AS failed_checks
FROM quakes.dq_results
GROUP BY run_id, run_at;
