IF OBJECT_ID(N'dw.dim_date', N'U') IS NULL
CREATE TABLE dw.dim_date (
    date_key   INT          NOT NULL CONSTRAINT pk_dim_date PRIMARY KEY,  -- YYYYMMDD, e.g. 20261123
    full_date  DATE         NOT NULL CONSTRAINT uq_dim_date_full_date UNIQUE,
    year       SMALLINT     NOT NULL,
    quarter    TINYINT      NOT NULL,
    month      TINYINT      NOT NULL,
    month_name NVARCHAR(20) NOT NULL,
    day        TINYINT      NOT NULL,
    day_name   NVARCHAR(20) NOT NULL
);

IF OBJECT_ID(N'dw.dim_country', N'U') IS NULL
CREATE TABLE dw.dim_country (
    country_key  INT IDENTITY(1, 1) NOT NULL CONSTRAINT pk_dim_country PRIMARY KEY,
    country_name NVARCHAR(100)      NOT NULL CONSTRAINT uq_dim_country_name UNIQUE
);
