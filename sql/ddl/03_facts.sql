-- One row per date + hour (UTC). hour_of_day is stored in the fact itself (no hour dimension).
IF OBJECT_ID(N'dw.fact_hourly_traffic', N'U') IS NULL
CREATE TABLE dw.fact_hourly_traffic (
    date_key         INT          NOT NULL CONSTRAINT fk_hourly_date REFERENCES dw.dim_date (date_key),
    hour_of_day      TINYINT      NOT NULL,
    aircraft_count   INT          NOT NULL,
    position_count   INT          NOT NULL,
    avg_velocity_kmh DECIMAL(9,1) NULL,   -- null when no aircraft was in the air
    avg_altitude_m   DECIMAL(9,1) NULL,
    loaded_at        DATETIME2    NOT NULL CONSTRAINT df_hourly_loaded_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_fact_hourly_traffic PRIMARY KEY (date_key, hour_of_day)
);

-- One row per date + origin country. activity_rank 1 = the most active country that day.
IF OBJECT_ID(N'dw.fact_country_traffic', N'U') IS NULL
CREATE TABLE dw.fact_country_traffic (
    date_key         INT          NOT NULL CONSTRAINT fk_country_date REFERENCES dw.dim_date (date_key),
    country_key      INT          NOT NULL CONSTRAINT fk_country_country REFERENCES dw.dim_country (country_key),
    aircraft_count   INT          NOT NULL,
    position_count   INT          NOT NULL,
    avg_velocity_kmh DECIMAL(9,1) NULL,
    avg_altitude_m   DECIMAL(9,1) NULL,
    activity_rank    INT          NOT NULL,
    loaded_at        DATETIME2    NOT NULL CONSTRAINT df_country_loaded_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_fact_country_traffic PRIMARY KEY (date_key, country_key)
);
