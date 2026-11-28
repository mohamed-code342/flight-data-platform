-- Plain copies of the Gold tables. Python empties and refills them on every load.
IF OBJECT_ID(N'stg.hourly_traffic', N'U') IS NULL
CREATE TABLE stg.hourly_traffic (
    load_date        DATE         NOT NULL,
    hour_of_day      TINYINT      NOT NULL,
    aircraft_count   INT          NOT NULL,
    position_count   INT          NOT NULL,
    avg_velocity_kmh DECIMAL(9,1) NULL,
    avg_altitude_m   DECIMAL(9,1) NULL
);

IF OBJECT_ID(N'stg.country_traffic', N'U') IS NULL
CREATE TABLE stg.country_traffic (
    load_date        DATE          NOT NULL,
    origin_country   NVARCHAR(100) NOT NULL,
    aircraft_count   INT           NOT NULL,
    position_count   INT           NOT NULL,
    avg_velocity_kmh DECIMAL(9,1)  NULL,
    avg_altitude_m   DECIMAL(9,1)  NULL,
    activity_rank    INT           NOT NULL
);
