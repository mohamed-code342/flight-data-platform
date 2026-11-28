-- Replace the whole date: delete what we loaded before for the dates in staging, then insert.
-- Running the load twice gives the same result (the Python loader wraps this in one transaction).
DELETE f
FROM dw.fact_hourly_traffic AS f
JOIN dw.dim_date AS d ON d.date_key = f.date_key
WHERE d.full_date IN (SELECT load_date FROM stg.hourly_traffic);

INSERT INTO dw.fact_hourly_traffic
    (date_key, hour_of_day, aircraft_count, position_count, avg_velocity_kmh, avg_altitude_m)
SELECT d.date_key, s.hour_of_day, s.aircraft_count, s.position_count,
       s.avg_velocity_kmh, s.avg_altitude_m
FROM stg.hourly_traffic AS s
JOIN dw.dim_date AS d ON d.full_date = s.load_date;
