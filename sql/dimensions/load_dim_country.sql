-- Add any country that appears in staging and is not in the dimension yet.
INSERT INTO dw.dim_country (country_name)
SELECT DISTINCT s.origin_country
FROM stg.country_traffic AS s
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_country AS c WHERE c.country_name = s.origin_country);
