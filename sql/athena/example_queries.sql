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

-- Data quality: one row per run (uses the quakes.dq_runs view)
SELECT run_at, published, errors, warnings, row_count, hours_since_newest_event, failed_checks
FROM quakes.dq_runs
ORDER BY run_at DESC
LIMIT 30;

-- JMA: how much more of Japan's activity does JMA see than USGS? Quakes per year near Japan.
SELECT j.year, j.jma_quakes, u.usgs_quakes, round(1.0 * j.jma_quakes / u.usgs_quakes, 1) AS ratio
FROM (SELECT year, count(*) AS jma_quakes FROM quakes.jma_events
      WHERE year >= 2015 AND event_type = 'earthquake'
        AND latitude BETWEEN 24 AND 46 AND longitude BETWEEN 122 AND 150
      GROUP BY year) j
JOIN (SELECT year, count(*) AS usgs_quakes FROM quakes.usgs_events
      WHERE year >= 2015 AND latitude BETWEEN 24 AND 46 AND longitude BETWEEN 122 AND 150
      GROUP BY year) u USING (year)
ORDER BY j.year;

-- Same quake, two magnitudes: how USGS magnitudes (mostly Mw) compare with JMA's Mj.
SELECT floor(jma_mag) AS jma_band, count(*) AS quakes,
       round(approx_percentile(mag_diff, 0.5), 2) AS median_usgs_minus_jma,
       round(avg(distance_km), 1) AS avg_distance_km, round(avg(time_diff_s), 2) AS avg_time_diff_s
FROM quakes.jma_usgs_matches
GROUP BY 1 ORDER BY 1;

-- Strongest shaking felt in Japan recently, with JMA's shindo.
SELECT event_time AT TIME ZONE 'Asia/Tokyo' AS time_jst, max_intensity AS shindo, mag, region_en
FROM quakes.jma_felt
WHERE year >= year(current_date) - 1
ORDER BY max_intensity DESC, event_time DESC
LIMIT 20;
