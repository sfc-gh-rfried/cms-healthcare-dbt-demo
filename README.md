# CMS Healthcare dbt Demo

A demonstration dbt project for Snowflake's **dbt Projects on Snowflake** feature. It is fully self-contained: small synthetic seed data, no dbt packages, and no network access, so it runs in locked-down accounts.

## Features

- **Seeds**: Synthetic patients, providers, claims, and conditions (`scripts/gen_seeds.py`)
- **Staging Models**: Clean and standardize raw healthcare data
- **Intermediate Models**: Business logic and aggregations
- **Mart Models**: Dimensional models for analytics, including an incremental fact table
- **Source freshness** on the seeded tables (table metadata, no timestamp column needed)
- **Optional Cortex model** (`fct_chronic_conditions`, AI_FILTER), off by default

## Data

| Seed | Rows | Notes |
|------|------|-------|
| raw_patients | 200 | No identity fields (SSN, license, passport) |
| raw_providers | 25 | |
| raw_claims | 1,000 | Service dates in the 90 days before generation |
| raw_conditions | 300 | Text descriptions for the Cortex example |

All values are generated from a fixed random seed; nothing is copied from real or Marketplace data. Regenerate with current dates:

```bash
python3 scripts/gen_seeds.py              # or --as-of YYYY-MM-DD for reproducible output
```

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
No external access integration is needed: the project has no `packages.yml`.

### Execute
```sql
-- env.yml: dev (per-user schemas, default), prod, single_schema (everything in one pre-created schema)
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO"
  ENVIRONMENT = 'dev' ARGS = 'build';               -- build runs seeds first

EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO"
  ENVIRONMENT = 'dev' ARGS = 'source freshness';    -- Fresh right after seeding

EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO"
  ENVIRONMENT = 'dev' ARGS = 'build --vars ''{enable_cortex_demo: true}''';  -- optional AI_FILTER model
```

### Versions
- dbt Core 1.12.3 (pinned). No dbt packages.
- Generic test arguments are nested under `arguments:`. Parses with no deprecation warnings.

## Models

| Layer | Model | Type |
|-------|-------|------|
| Seeds | raw_patients, raw_providers, raw_claims, raw_conditions | table |
| Staging | stg_cms__claims | view |
| Staging | stg_cms__patients | view |
| Staging | stg_cms__providers | view |
| Intermediate | int_cms__claims_summary | table |
| Marts | dim_cms__patients | table |
| Marts | dim_cms__providers | table |
| Marts | fct_cms__claims | incremental |
| Marts (optional) | fct_chronic_conditions | table, AI_FILTER |
