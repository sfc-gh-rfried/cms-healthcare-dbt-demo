# CMS Healthcare dbt Demo

A demonstration dbt project for Snowflake's **dbt Projects on Snowflake** feature, using synthetic healthcare data from the Snowflake Marketplace.

## Features

- **Staging Models**: Clean and standardize raw healthcare data
- **Intermediate Models**: Business logic and aggregations  
- **Mart Models**: Dimensional models for analytics
- **Package Examples**: Demonstrations of dbt_utils, dbt_date, dbt_expectations

## Data Source

Uses `SYNTHETIC_HEALTHCARE_DATA__CLINICAL_AND_CLAIMS.SILVER` from Snowflake Marketplace:
- 124M claims
- 1.4M patients
- 4K providers

## Quick Start

### Deploy to Snowflake
```bash
snow dbt deploy cms_healthcare_demo \
  --source . \
  --database DBT_CMS_DEMO \
  --schema DBT_PROJECTS \
  --dbt-version 1.12.3 \
  -c YOUR_CONNECTION
```

### Execute
```sql
-- env.yml defines dev (per-user schemas, default) and prod
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO"
  ENVIRONMENT = 'dev' ARGS = 'build';

EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO"
  ENVIRONMENT = 'dev' ARGS = 'source freshness';  -- CLAIMS reports STALE: the Marketplace data is a static snapshot
```

### Versions (2026 refresh)
- dbt Core 1.12.3 (pinned). Verified: `build` PASS=47.
- Packages: dbt_utils 1.4.1, metaplane/dbt_expectations 0.10.10, godatadriven/dbt_date 0.21.0 (see `package-lock.yml`).
- Generic test arguments are nested under `arguments:`. Parses with no deprecation warnings.

## Models

| Layer | Model | Type |
|-------|-------|------|
| Staging | stg_cms__claims | view |
| Staging | stg_cms__patients | view |
| Staging | stg_cms__providers | view |
| Intermediate | int_cms__claims_summary | table |
| Marts | dim_cms__patients | table |
| Marts | dim_cms__providers | table |
| Marts | fct_cms__claims | incremental |
| Examples | example_dbt_utils | view |
| Examples | example_dbt_date | table |
| Examples | example_dbt_expectations | view |
