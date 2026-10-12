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
    value     double,   -- the measured number (row count, hours since newest event, ...)
    dataset   string    -- 'usgs' or 'jma' (null in results written before JMA was added = usgs)
)
STORED AS PARQUET
LOCATION 's3://gabe-quake-pipeline/data/dq/results/';

-- If quakes.dq_results already exists from before JMA was added, run this once instead:
-- ALTER TABLE quakes.dq_results ADD COLUMNS (dataset string);

-- One row per dataset per daily run, pivoted from dq_results for easy reading.
-- dq_results stays in long format (one row per check) so new checks never
-- change its schema; this view presents the same data wide.
CREATE OR REPLACE VIEW quakes.dq_runs AS
SELECT
    run_id,
    min(run_at)                                              AS run_at,
    coalesce(dataset, 'usgs')                                AS dataset,
    bool_and(passed OR severity = 'warn')                    AS published,
    count_if(NOT passed AND severity = 'error')              AS errors,
    count_if(NOT passed AND severity = 'warn')               AS warnings,
    max(CASE WHEN "check" = 'not empty'     THEN value END)  AS row_count,
    max(CASE WHEN "check" = 'data is fresh' THEN value END)  AS hours_since_newest_event,
    array_join(array_agg(CASE WHEN NOT passed THEN "check" END), ', ') AS failed_checks
FROM quakes.dq_results
GROUP BY run_id, coalesce(dataset, 'usgs');

-- ---------------------------------------------------------------------------
-- JMA (Japan Meteorological Agency)
-- ---------------------------------------------------------------------------

-- Every earthquake JMA located: the final catalog for the years it covers, then
-- preliminary daily lists (status = 'final' / 'preliminary'). Times are UTC.
CREATE EXTERNAL TABLE IF NOT EXISTS quakes.jma_events (
    event_id       string,
    event_time     timestamp,
    latitude       double,
    longitude      double,
    depth_km       double,
    mag            double,    -- null for quakes too small to measure
    mag_type       string,    -- mostly Mj, JMA's own magnitude scale
    max_intensity  string,    -- shindo (震度, seismic intensity) 1..7, catalog years only
    event_type     string,    -- earthquake / artificial / volcanic / low_frequency
    region         string,    -- as published: English (catalog) or Japanese (daily lists)
    region_en      string,    -- English region name, filled in for daily-list rows
    agency         string,    -- J = JMA; U = USGS, I = other (far-away quakes in the catalog)
    status         string,
    source         string
)
PARTITIONED BY (year int)
STORED AS PARQUET
LOCATION 's3://gabe-quake-pipeline/data/clean/jma/'
TBLPROPERTIES (
    'projection.enabled'        = 'true',
    'projection.year.type'      = 'integer',
    'projection.year.range'     = '1919,2100',
    'storage.location.template' = 's3://gabe-quake-pipeline/data/clean/jma/year=${year}/'
);

-- Earthquakes felt in Japan, with shindo, archived from JMA's near-real-time feed.
CREATE EXTERNAL TABLE IF NOT EXISTS quakes.jma_felt (
    report_id      string,
    event_time     timestamp,  -- to the minute, as the feed gives it
    latitude       double,
    longitude      double,
    depth_km       double,
    mag            double,
    max_intensity  string,
    region         string,
    region_en      string,
    updated_at     timestamp
)
PARTITIONED BY (year int)
STORED AS PARQUET
LOCATION 's3://gabe-quake-pipeline/data/clean/jma_felt/'
TBLPROPERTIES (
    'projection.enabled'        = 'true',
    'projection.year.type'      = 'integer',
    'projection.year.range'     = '2026,2100',
    'storage.location.template' = 's3://gabe-quake-pipeline/data/clean/jma_felt/year=${year}/'
);

-- Links between a JMA earthquake and the same earthquake in USGS (matched on time and distance).
CREATE EXTERNAL TABLE IF NOT EXISTS quakes.jma_usgs_matches (
    jma_event_id    string,
    usgs_event_id   string,
    jma_event_time  timestamp,
    time_diff_s     double,
    distance_km     double,
    jma_mag         double,
    jma_mag_type    string,
    usgs_mag        double,
    usgs_mag_type   string,
    mag_diff        double,    -- usgs_mag - jma_mag
    jma_depth_km    double,
    usgs_depth_km   double,
    score           double     -- 0 = same time and place; matches are kept up to 2
)
STORED AS PARQUET
LOCATION 's3://gabe-quake-pipeline/data/clean/jma_usgs_matches/';
