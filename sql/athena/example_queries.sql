-- Example queries. Filtering on `year` means Athena only reads those folders,
-- which keeps queries fast and cheap.

-- Earthquakes per year since 2000
SELECT year, count(*) AS quakes, round(avg(mag), 2) AS avg_mag
FROM quakes.usgs_events
WHERE year >= 2000
GROUP BY year
ORDER BY year;

-- Strongest earthquakes near Japan in the last 10 years
SELECT event_time, mag, depth_km, place
FROM quakes.usgs_events
WHERE year >= year(current_date) - 10
  AND latitude BETWEEN 24 AND 46
  AND longitude BETWEEN 122 AND 146
ORDER BY mag DESC
LIMIT 20;

-- Most recent events (checks the daily refresh is working)
SELECT event_time, mag, place, updated_at
FROM quakes.usgs_events
WHERE year = year(current_date)
ORDER BY event_time DESC
LIMIT 20;

-- Data quality: failed or warning checks in the last 30 days
SELECT run_at, "check", severity, observed, expected
FROM quakes.dq_results
WHERE NOT passed
  AND run_at > current_timestamp - INTERVAL '30' DAY
ORDER BY run_at DESC;

-- Data quality: row count and freshness trend, one row per run
SELECT run_at,
       max(CASE WHEN "check" = 'not empty' THEN value END)     AS row_count,
       max(CASE WHEN "check" = 'data is fresh' THEN value END) AS hours_since_newest_event,
       bool_and(passed OR severity = 'warn')                    AS published
FROM quakes.dq_results
GROUP BY run_at
ORDER BY run_at DESC;
