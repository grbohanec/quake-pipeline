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
