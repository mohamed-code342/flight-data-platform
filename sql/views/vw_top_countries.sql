-- The 10 most active countries of each day (ready for a bar chart).
CREATE OR ALTER VIEW bi.vw_top_countries AS
SELECT
    d.full_date,
    c.country_name,
    f.aircraft_count,
    f.position_count,
    f.avg_velocity_kmh,
    f.avg_altitude_m,
    f.activity_rank
FROM dw.fact_country_traffic AS f
JOIN dw.dim_date AS d ON d.date_key = f.date_key
JOIN dw.dim_country AS c ON c.country_key = f.country_key
WHERE f.activity_rank <= 10;
