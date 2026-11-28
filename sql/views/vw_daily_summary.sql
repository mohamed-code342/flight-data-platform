-- One row per day for the KPI cards.
-- total_aircraft is a sum of per-country counts. That is exact because every aircraft
-- has a single origin country, so it equals the number of distinct aircraft that day.
-- Never SUM aircraft_count across hours instead: the same aircraft appears in many hours.
CREATE OR ALTER VIEW bi.vw_daily_summary AS
SELECT
    d.full_date,
    d.day_name,
    SUM(f.aircraft_count) AS total_aircraft,
    SUM(f.position_count) AS total_positions,
    COUNT(*)              AS countries_count
FROM dw.fact_country_traffic AS f
JOIN dw.dim_date AS d ON d.date_key = f.date_key
GROUP BY d.full_date, d.day_name;
