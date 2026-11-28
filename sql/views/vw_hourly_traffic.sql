-- Traffic per hour (UTC). Power BI: line charts of aircraft, speed and altitude over the day.
CREATE OR ALTER VIEW bi.vw_hourly_traffic AS
SELECT
    d.full_date,
    d.day_name,
    f.hour_of_day,
    RIGHT('0' + CAST(f.hour_of_day AS VARCHAR(2)), 2) + ':00' AS hour_label,
    f.aircraft_count,
    f.position_count,
    f.avg_velocity_kmh,
    f.avg_altitude_m
FROM dw.fact_hourly_traffic AS f
JOIN dw.dim_date AS d ON d.date_key = f.date_key;
