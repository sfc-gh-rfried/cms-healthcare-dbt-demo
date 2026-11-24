{{ config(materialized='table', enabled=var('enable_cortex_demo', false)) }}

-- Optional: Cortex AI function inside a dbt model. Off by default because Cortex
-- availability varies by account/region. Enable with: --vars '{enable_cortex_demo: true}'
-- Uses the text description: AI_FILTER gave wrong answers on bare diagnosis codes.
SELECT
    CONDITION_ID AS condition_id,
    PATIENT AS patient_id,
    CODE AS condition_code,
    DESCRIPTION AS condition_description,
    START_DATE AS onset_date
FROM {{ ref('raw_conditions') }}
WHERE AI_FILTER(PROMPT('This diagnosis is a chronic condition: {0}', DESCRIPTION))
