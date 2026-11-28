DELETE f
FROM dw.fact_country_traffic AS f
JOIN dw.dim_date AS d ON d.date_key = f.date_key
WHERE d.full_date IN (SELECT load_date FROM stg.country_traffic);

INSERT INTO dw.fact_country_traffic
    (date_key, country_key, aircraft_count, position_count,
     avg_velocity_kmh, avg_altitude_m, activity_rank)
SELECT d.date_key, c.country_key, s.aircraft_count, s.position_count,
       s.avg_velocity_kmh, s.avg_altitude_m, s.activity_rank
FROM stg.country_traffic AS s
JOIN dw.dim_date AS d ON d.full_date = s.load_date
JOIN dw.dim_country AS c ON c.country_name = s.origin_country;
