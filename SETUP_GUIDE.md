# CMS Healthcare dbt Demo - Workspace Setup Guide

## Step 1: Create Workspace in Snowsight

1. Open Snowsight and navigate to **Projects > Workspaces**
2. Click **+ Workspace** dropdown
3. Select **Create Workspace**
4. Name: `cms_healthcare_demo`
5. Click **Create**

## Step 2: Upload Files to Workspace

Upload these files from your local project:
```
/projects/dbt_demo/customer/snowsight_demo/
```

### File Structure to Create:

```
cms_healthcare_demo/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── macros/
│   └── generate_schema_name.sql
└── models/
    ├── staging/
    │   ├── _sources.yml
    │   ├── _schema.yml
    │   ├── stg_cms__patients.sql
    │   ├── stg_cms__providers.sql
    │   └── stg_cms__claims.sql
    ├── intermediate/
    │   ├── _schema.yml
    │   └── int_cms__claims_summary.sql
    └── marts/
        ├── _schema.yml
        ├── dim_cms__patients.sql
        ├── dim_cms__providers.sql
        └── fct_cms__claims.sql
```

## Step 3: Run dbt Commands

### 3.1 Install Dependencies
1. Select **Project**: `cms_demo`
2. Select **Target**: `dev`
3. Select **Command**: `deps`
4. Enter External Access Integration name (if required): `dbt_access_integration`
5. Click **Play**

### 3.2 Compile and Verify
1. Select **Command**: `compile`
2. Click **Play**
3. Click **DAG** to view model dependencies

### 3.3 Run Models
```
# Run staging first
dbt run --select staging

# Run intermediate
dbt run --select intermediate

# Run marts
dbt run --select marts

# Or run all at once
dbt run
```

### 3.4 Run Tests
```
dbt test
```

## Step 4: Deploy as DBT PROJECT

1. Click **Connect** > **Deploy dbt project**
2. Select database: `DBT_CMS_DEMO`
3. Select schema: `DBT_PROJECTS`
4. Name: `cms_healthcare_demo`
5. Default target: `prod`
6. Click **Deploy**

## Step 5: Verify Deployment

```sql
-- List deployed projects
SHOW DBT PROJECTS IN SCHEMA DBT_CMS_DEMO.DBT_PROJECTS;

-- Describe project
DESCRIBE DBT PROJECT DBT_CMS_DEMO.DBT_PROJECTS.cms_healthcare_demo;
```

## Step 6: Execute via SQL

```sql
-- Run all models
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."cms_healthcare_demo"
  args='run --target prod';

-- Run specific models
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."cms_healthcare_demo"
  args='run --select staging --target prod';

-- Run tests
EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."cms_healthcare_demo"
  args='test --target prod';
```

## Step 7: Create Task DAG (Optional)

```sql
-- Root task: dbt run
CREATE OR REPLACE TASK DBT_CMS_DEMO.DBT_PROJECTS.dbt_cms_run
  WAREHOUSE = XSMALL_WH
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  AS EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."cms_healthcare_demo"
     args='run --target prod';

-- Dependent task: dbt test
CREATE OR REPLACE TASK DBT_CMS_DEMO.DBT_PROJECTS.dbt_cms_test
  WAREHOUSE = XSMALL_WH
  AFTER DBT_CMS_DEMO.DBT_PROJECTS.dbt_cms_run
  AS EXECUTE DBT PROJECT "DBT_CMS_DEMO"."DBT_PROJECTS"."cms_healthcare_demo"
     args='test --target prod';

-- Resume tasks
ALTER TASK DBT_CMS_DEMO.DBT_PROJECTS.dbt_cms_test RESUME;
ALTER TASK DBT_CMS_DEMO.DBT_PROJECTS.dbt_cms_run RESUME;

-- Manual execution
EXECUTE TASK DBT_CMS_DEMO.DBT_PROJECTS.dbt_cms_run;
```

## Step 8: Enable Monitoring

```sql
ALTER SCHEMA DBT_CMS_DEMO.DBT_PROJECTS SET LOG_LEVEL = 'INFO';
ALTER SCHEMA DBT_CMS_DEMO.DBT_PROJECTS SET TRACE_LEVEL = 'ALWAYS';
```

## Step 9: View Results

Navigate to **Transformation > dbt Projects** to see:
- Run history
- Success/failure status
- Execution details

---

## Troubleshooting

### "env_var required but not provided"
The profiles.yml uses placeholders - this is expected for Snowsight-native execution.

### "Schema does not exist"
Run these first:
```sql
CREATE SCHEMA IF NOT EXISTS DBT_CMS_DEMO.STAGING;
CREATE SCHEMA IF NOT EXISTS DBT_CMS_DEMO.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS DBT_CMS_DEMO.MARTS;
CREATE SCHEMA IF NOT EXISTS DBT_CMS_DEMO.DEV;
CREATE SCHEMA IF NOT EXISTS DBT_CMS_DEMO.PROD;
```

### "dbt deps failed"
Create External Access Integration:
```sql
CREATE OR REPLACE NETWORK RULE dbt_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'codeload.github.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_access_integration
  ALLOWED_NETWORK_RULES = (dbt_network_rule)
  ENABLED = TRUE;
```
