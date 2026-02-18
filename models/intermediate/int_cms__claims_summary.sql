WITH claims AS (
    SELECT * FROM {{ ref('stg_cms__claims') }}
),

aggregated AS (
    SELECT
        patient_id,
        COUNT(DISTINCT claim_id) AS total_claims,
        COUNT(DISTINCT provider_id) AS unique_providers,
        COUNT(DISTINCT service_date) AS service_days,
        SUM(COALESCE(outstanding_amount_1, 0) + 
            COALESCE(outstanding_amount_2, 0) + 
            COALESCE(outstanding_amount_p, 0)) AS total_outstanding,
        MIN(service_date) AS first_service_date,
        MAX(service_date) AS last_service_date,
        COUNT(DISTINCT diagnosis_code_1) AS unique_primary_diagnoses
    FROM claims
    WHERE patient_id IS NOT NULL
    GROUP BY patient_id
)

SELECT
    MD5(CAST(patient_id AS VARCHAR)) AS patient_claims_key,
    patient_id,
    total_claims,
    unique_providers,
    service_days,
    total_outstanding,
    first_service_date,
    last_service_date,
    unique_primary_diagnoses,
    DATEDIFF('day', first_service_date, last_service_date) AS care_span_days,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM aggregated
