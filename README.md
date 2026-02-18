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
  -c YOUR_CONNECTION
```

### Execute
```sql
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO" 
  args='run --target dev';
```

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
