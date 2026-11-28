-- Traffic per origin country (country of registration) per day, with its activity rank.
CREATE OR ALTER VIEW bi.vw_country_traffic AS
SELECT
    d.full_date,
    d.day_name,
    c.country_name,
    f.aircraft_count,
    f.position_count,
    f.avg_velocity_kmh,
    f.avg_altitude_m,
    f.activity_rank
FROM dw.fact_country_traffic AS f
JOIN dw.dim_date AS d ON d.date_key = f.date_key
JOIN dw.dim_country AS c ON c.country_key = f.country_key;
