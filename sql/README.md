# SQL

| Folder | What is inside | When it runs |
|---|---|---|
| `ddl/` | schemas (`dw`, `stg`), dimension and fact tables | once: `make warehouse-init` (safe to re-run) |
| `staging/` | staging tables (`stg.*`) | once, with `ddl/` |
| `dimensions/` | add new dates and countries to the dimensions | every load |
| `facts/` | replace the loaded dates in the fact tables | every load |
| `views/` | flat views in schema `bi` for Power BI (one view per file) | once, with `ddl/` |

Files run in alphabetical order. Do not use `GO` in them: each file is sent as one batch
(that is why every view has its own file).

## Star schema

```
dim_date (date_key) ----< fact_hourly_traffic   (date_key, hour_of_day)
dim_date (date_key) ----< fact_country_traffic  (date_key, country_key) >---- dim_country (country_key)
```
