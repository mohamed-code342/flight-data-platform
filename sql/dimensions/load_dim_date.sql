-- Add any date that appears in staging and is not in the dimension yet.
INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, month_name, day, day_name)
SELECT
    YEAR(d.load_date) * 10000 + MONTH(d.load_date) * 100 + DAY(d.load_date),
    d.load_date,
    YEAR(d.load_date),
    DATEPART(QUARTER, d.load_date),
    MONTH(d.load_date),
    DATENAME(MONTH, d.load_date),
    DAY(d.load_date),
    DATENAME(WEEKDAY, d.load_date)
FROM (
    SELECT load_date FROM stg.hourly_traffic
    UNION
    SELECT load_date FROM stg.country_traffic
) AS d
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date AS x WHERE x.full_date = d.load_date);
