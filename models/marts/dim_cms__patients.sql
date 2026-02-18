WITH patients AS (
    SELECT * FROM {{ ref('stg_cms__patients') }}
),

claims_summary AS (
    SELECT * FROM {{ ref('int_cms__claims_summary') }}
),

joined AS (
    SELECT
        p.*,
        cs.total_claims,
        cs.unique_providers,
        cs.service_days,
        cs.total_outstanding,
        cs.first_service_date,
        cs.last_service_date,
        cs.care_span_days
    FROM patients p
    LEFT JOIN claims_summary cs ON p.patient_id = cs.patient_id
)

SELECT
    MD5(CAST(patient_id AS VARCHAR)) AS patient_key,
    patient_id,
    first_name,
    last_name,
    birth_date,
    death_date,
    gender,
    race,
    ethnicity,
    city,
    state,
    zip_code,
    lifetime_healthcare_expenses,
    lifetime_healthcare_coverage,
    COALESCE(total_claims, 0) AS total_claims,
    COALESCE(unique_providers, 0) AS unique_providers,
    COALESCE(service_days, 0) AS service_days,
    first_service_date,
    last_service_date,
    COALESCE(care_span_days, 0) AS care_span_days,
    CASE
        WHEN total_claims IS NULL OR total_claims = 0 THEN 'Low'
        WHEN total_claims < 10 THEN 'Low'
        WHEN total_claims < 50 THEN 'Medium'
        WHEN total_claims < 100 THEN 'High'
        ELSE 'Very High'
    END AS utilization_tier,
    CASE WHEN death_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_deceased,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM joined
