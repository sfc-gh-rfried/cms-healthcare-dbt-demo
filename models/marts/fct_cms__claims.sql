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
    diagnosis_code_2,
    diagnosis_code_3,
    outstanding_amount_1,
    outstanding_amount_2,
    outstanding_amount_p,
    outstanding_amount_1 + COALESCE(outstanding_amount_2, 0) + COALESCE(outstanding_amount_p, 0) AS total_outstanding,
    claim_status_1,
    claim_type_id_1,
    (CASE WHEN diagnosis_code_1 IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN diagnosis_code_2 IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN diagnosis_code_3 IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN diagnosis_code_4 IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN diagnosis_code_5 IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN diagnosis_code_6 IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN diagnosis_code_7 IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN diagnosis_code_8 IS NOT NULL THEN 1 ELSE 0 END) AS diagnosis_count,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM claims
