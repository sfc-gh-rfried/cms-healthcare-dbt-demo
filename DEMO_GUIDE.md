# dbt Projects on Snowflake: CMS Healthcare Demo

## Introduction

**dbt Projects on Snowflake** enables you to build, test, deploy, and monitor data transformations using dbt—directly within Snowflake. No external infrastructure required.

### Why dbt Projects on Snowflake?

| Benefit | Description |
|---------|-------------|
| **Zero Infrastructure** | No dbt Cloud subscription or self-hosted servers needed |
| **Native Integration** | Runs on your existing Snowflake warehouse |
| **Snowsight Workspaces** | Modern IDE experience with Git integration |
| **Built-in Orchestration** | Schedule with Tasks, monitor with Snowflake observability |

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DEVELOPMENT FLOW                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   Local IDE / VS Code          Snowsight Workspace                  │
│         │                              │                             │
│         │    ┌──────────────┐          │                             │
│         └───►│   GitHub     │◄─────────┘                             │
│              │   Repository │                                        │
│              └──────┬───────┘                                        │
│                     │                                                │
│                     ▼                                                │
│              ┌──────────────┐                                        │
│              │ Git Repository│  (Snowflake Object)                   │
│              │    Object     │                                        │
│              └──────┬───────┘                                        │
│                     │                                                │
│                     ▼                                                │
│              ┌──────────────┐                                        │
│              │ DBT PROJECT  │  CREATE DBT PROJECT ... FROM @repo     │
│              │    Object    │                                        │
│              └──────┬───────┘                                        │
│                     │                                                │
│                     ▼                                                │
│              ┌──────────────┐                                        │
│              │   EXECUTE    │  EXECUTE DBT PROJECT args='run'        │
│              │  DBT PROJECT │                                        │
│              └──────┬───────┘                                        │
│                     │                                                │
│                     ▼                                                │
│              ┌──────────────┐                                        │
│              │ Tables/Views │  Materialized in target schemas        │
│              └──────────────┘                                        │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Components

1. **Snowsight Workspace** - Browser-based IDE for editing dbt files
2. **Git Repository Object** - Syncs with GitHub/GitLab for version control
3. **DBT PROJECT Object** - Deployed, versioned dbt project in Snowflake
4. **EXECUTE DBT PROJECT** - Runs dbt commands (run, test, build, etc.)

---

## Our Data Model: CMS Healthcare Analytics

We'll transform synthetic healthcare data from the Snowflake Marketplace into a dimensional model for analytics.

### Source Data

**Database**: `SYNTHETIC_HEALTHCARE_DATA__CLINICAL_AND_CLAIMS.SILVER`

| Table | Rows | Description |
|-------|------|-------------|
| CLAIMS | 124M | Insurance claims with diagnoses, costs |
| PATIENTS | 1.4M | Patient demographics |
| PROVIDERS | 4K | Healthcare providers |

### Target Model

```
                    ┌─────────────────────────────────────┐
                    │           SOURCE LAYER              │
                    │  (Snowflake Marketplace Data)       │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────┴───────────────────┐
                    │           STAGING LAYER             │
                    │  stg_cms__claims                    │
                    │  stg_cms__patients                  │
                    │  stg_cms__providers                 │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────┴───────────────────┐
                    │        INTERMEDIATE LAYER           │
                    │  int_cms__claims_summary            │
                    └─────────────────┬───────────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          │                           │                           │
          ▼                           ▼                           ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│ dim_cms__patients│       │dim_cms__providers│       │ fct_cms__claims │
│   (Dimension)   │       │   (Dimension)    │       │  (Incremental)  │
└─────────────────┘       └─────────────────┘       └─────────────────┘
```

---

## Step-by-Step Walkthrough

### Step 1: Project Structure

Every dbt project starts with two essential files:

```
cms_healthcare_demo/
├── dbt_project.yml      # Project configuration
├── profiles.yml         # Connection settings
├── models/
│   ├── staging/         # Clean raw data
│   ├── intermediate/    # Business logic
│   └── marts/           # Analytics-ready tables
└── macros/              # Reusable SQL snippets
```

#### dbt_project.yml
```yaml
name: 'cms_demo'
version: '1.0.0'
config-version: 2
profile: 'cms_demo'

model-paths: ["models"]
macro-paths: ["macros"]

models:
  cms_demo:
    staging:
      +materialized: view
      +schema: STAGING
    intermediate:
      +materialized: table
      +schema: INTERMEDIATE
    marts:
      +materialized: table
      +schema: MARTS
```

#### profiles.yml (Snowflake-specific)
```yaml
cms_demo:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: 'not needed'    # Snowflake handles this
      user: 'not needed'       # Uses session context
      role: SYSADMIN
      database: DBT_CMS_DEMO
      warehouse: XSMALL_WH
      schema: DEV
```

> **Important**: When running in Snowflake Workspaces, `account` and `user` are automatically provided by the session. Use placeholder values.

---

### Step 2: Define Sources

Before writing models, declare your source tables in `_sources.yml`:

#### models/staging/_sources.yml
```yaml
version: 2

sources:
  - name: medicare
    database: SYNTHETIC_HEALTHCARE_DATA__CLINICAL_AND_CLAIMS
    schema: SILVER
    description: "CMS Synthetic Healthcare Data from Snowflake Marketplace"
    tables:
      - name: PATIENTS
        description: "Patient demographics"
        columns:
          - name: PATIENT_ID
            description: "Unique patient identifier"
            data_tests:
              - unique
              - not_null
              
      - name: PROVIDERS
        description: "Healthcare providers"
        columns:
          - name: PROVIDER_ID
            data_tests:
              - unique
              - not_null
              
      - name: CLAIMS
        description: "Insurance claims"
        columns:
          - name: CLAIM_ID
            data_tests:
              - unique
              - not_null
```

**Why sources?**
- Documents where data comes from
- Enables `{{ source() }}` function in models
- Allows testing source data quality
- Supports lineage tracking

---

### Step 3: Create Staging Models

Staging models clean and standardize raw data. They should:
- Rename columns to consistent conventions
- Cast data types explicitly
- Add audit columns

#### models/staging/stg_cms__claims.sql
```sql
WITH source AS (
    SELECT * FROM {{ source('medicare', 'CLAIMS') }}
),

staged AS (
    SELECT
        -- Primary key
        CLAIM_ID AS claim_id,
        
        -- Foreign keys
        PATIENT_ID AS patient_id,
        PROVIDER_ID AS provider_id,
        
        -- Insurance info
        PRIMARY_PATIENT_INSURANCE_ID AS primary_insurance_id,
        SECONDARY_PATIENT_INSURANCE_ID AS secondary_insurance_id,
        
        -- Diagnosis codes (SNOMED-CT)
        DIAGNOSIS1 AS diagnosis_code_1,
        DIAGNOSIS2 AS diagnosis_code_2,
        DIAGNOSIS3 AS diagnosis_code_3,
        
        -- Dates
        SERVICEDATE AS service_date,
        CURRENTILLNESSDATE AS current_illness_date,
        
        -- Financials
        OUTSTANDING1 AS outstanding_amount_1,
        OUTSTANDING2 AS outstanding_amount_2,
        OUTSTANDINGP AS outstanding_amount_p,
        
        -- Status
        STATUS1 AS claim_status_1,
        
        -- Audit
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)

SELECT * FROM staged
```

**Key patterns:**
- `{{ source('medicare', 'CLAIMS') }}` references the source definition
- Consistent snake_case naming
- Audit column `_loaded_at` for tracking

---

### Step 4: Add Schema Tests

Define tests alongside models in `_schema.yml`:

#### models/staging/_schema.yml
```yaml
version: 2

models:
  - name: stg_cms__claims
    description: "Staged claims data with standardized column names"
    columns:
      - name: claim_id
        description: "Unique claim identifier"
        data_tests:
          - unique
          - not_null
          
      - name: patient_id
        data_tests:
          - not_null
          
      - name: service_date
        description: "Date services were rendered"
```

---

### Step 5: Build Intermediate Models

Intermediate models apply business logic and aggregations:

#### models/intermediate/int_cms__claims_summary.sql
```sql
WITH claims AS (
    SELECT * FROM {{ ref('stg_cms__claims') }}
),

aggregated AS (
    SELECT
        patient_id,
        COUNT(DISTINCT claim_id) AS total_claims,
        COUNT(DISTINCT provider_id) AS unique_providers,
        SUM(COALESCE(outstanding_amount_1, 0) + 
            COALESCE(outstanding_amount_2, 0) + 
            COALESCE(outstanding_amount_p, 0)) AS total_outstanding,
        MIN(service_date) AS first_service_date,
        MAX(service_date) AS last_service_date
    FROM claims
    WHERE patient_id IS NOT NULL
    GROUP BY patient_id
)

SELECT
    MD5(CAST(patient_id AS VARCHAR)) AS patient_claims_key,
    patient_id,
    total_claims,
    unique_providers,
    total_outstanding,
    first_service_date,
    last_service_date,
    DATEDIFF('day', first_service_date, last_service_date) AS care_span_days,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM aggregated
```

**Key patterns:**
- `{{ ref('stg_cms__claims') }}` creates dependency on staging model
- `MD5()` generates surrogate keys (native Snowflake function)
- Business calculations (care_span_days)

---

### Step 6: Create Mart Models

Mart models are the final, analytics-ready tables:

#### models/marts/fct_cms__claims.sql (Incremental)
```sql
{{
    config(
        materialized='incremental',
        unique_key='claim_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

WITH claims AS (
    SELECT * FROM {{ ref('stg_cms__claims') }}
    {% if is_incremental() %}
    WHERE service_date > (SELECT MAX(service_date) FROM {{ this }})
    {% endif %}
)

SELECT
    MD5(CAST(claim_id AS VARCHAR)) AS claim_key,
    claim_id,
    patient_id,
    provider_id,
    service_date,
    diagnosis_code_1,
    outstanding_amount_1 + COALESCE(outstanding_amount_2, 0) 
        + COALESCE(outstanding_amount_p, 0) AS total_outstanding,
    claim_status_1,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM claims
```

**Incremental strategy:**
- First run: loads all data
- Subsequent runs: only new records where `service_date > MAX(service_date)`
- Uses `MERGE` for upserts based on `claim_key`

---

### Step 7: Custom Schema Macro

By default, dbt concatenates schema names. Override with:

#### macros/generate_schema_name.sql
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

This ensures models go to exactly `STAGING`, `INTERMEDIATE`, `MARTS`—not `DEV_STAGING`.

---

## Running the Project

### In Snowsight Workspace

```
┌────────────────────────────────────────────────────────────────┐
│  [Run ▼]  [Compile]  [Test]  [Build]     Target: [dev ▼]      │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Select command from dropdown:                                 │
│  • Run        - Execute models                                 │
│  • Compile    - Generate SQL without executing                 │
│  • Test       - Run data tests                                 │
│  • Build      - Run + Test in dependency order                 │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### Via SQL

```sql
-- Run all models
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO" 
  args='run --target dev';

-- Run specific model
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO" 
  args='run --select fct_cms__claims --target dev';

-- Run tests
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO" 
  args='test --target dev';

-- Full refresh incremental
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."CMS_HEALTHCARE_DEMO" 
  args='run --select fct_cms__claims --full-refresh --target dev';
```

### Via Snowflake CLI

```bash
snow dbt execute cms_healthcare_demo --args "run --target dev"
```

---

## Results

After running the project:

| Schema | Object | Type | Rows |
|--------|--------|------|------|
| STAGING | stg_cms__claims | VIEW | - |
| STAGING | stg_cms__patients | VIEW | - |
| STAGING | stg_cms__providers | VIEW | - |
| INTERMEDIATE | int_cms__claims_summary | TABLE | 1.4M |
| MARTS | dim_cms__patients | TABLE | 1.4M |
| MARTS | dim_cms__providers | TABLE | 4K |
| MARTS | fct_cms__claims | TABLE | 124M |

---

## Package Examples

The project also demonstrates popular dbt packages:

### dbt_utils - Surrogate Keys
```sql
{{ dbt_utils.generate_surrogate_key(['claim_id', 'patient_id']) }} AS claim_patient_key
```

### dbt_date - Date Dimensions
```sql
{{ dbt_utils.date_spine(datepart="day", start_date="...", end_date="...") }}
{{ dbt_date.day_of_week('date_day') }} AS day_of_week
```

### dbt_expectations - Data Quality
```yaml
columns:
  - name: outstanding_amount
    data_tests:
      - dbt_expectations.expect_column_values_to_be_between:
          min_value: 0
          max_value: 1000000
```

---

## Resources

- **GitHub Repo**: https://github.com/sfc-gh-rfried/cms-healthcare-dbt-demo
- **Snowflake Docs**: [dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- **dbt Docs**: [dbt Core](https://docs.getdbt.com/)

---

## Quick Reference

```sql
-- Show deployed dbt projects
SHOW DBT PROJECTS IN SCHEMA DBT_CMS_DEMO.DBT_PROJECTS;

-- Describe project
DESCRIBE DBT PROJECT DBT_CMS_DEMO.DBT_PROJECTS.CMS_HEALTHCARE_DEMO;

-- Get execution logs
SELECT * FROM TABLE(SYSTEM$GET_DBT_LOG('<query_id>'));

-- View run history
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DBT_PROJECT_RUN_HISTORY
WHERE PROJECT_NAME = 'CMS_HEALTHCARE_DEMO'
ORDER BY START_TIME DESC;
```
