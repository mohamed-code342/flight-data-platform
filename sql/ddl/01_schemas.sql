-- dw  = the warehouse tables (dimensions and facts)
-- stg = staging tables, emptied and refilled on every load
-- bi  = flat views that Power BI reads
IF SCHEMA_ID(N'dw') IS NULL EXEC(N'CREATE SCHEMA dw');
IF SCHEMA_ID(N'stg') IS NULL EXEC(N'CREATE SCHEMA stg');
IF SCHEMA_ID(N'bi') IS NULL EXEC(N'CREATE SCHEMA bi');
