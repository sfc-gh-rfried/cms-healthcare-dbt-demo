WITH providers AS (
    SELECT * FROM {{ ref('stg_cms__providers') }}
)

SELECT
    MD5(CAST(provider_id AS VARCHAR)) AS provider_key,
    provider_id,
    provider_name,
    organization_id,
    specialty,
    city,
    state,
    zip_code,
    encounter_count,
    procedure_count,
    CASE
        WHEN encounter_count IS NULL OR encounter_count = 0 THEN 'Inactive'
        WHEN encounter_count < 100 THEN 'Low Volume'
        WHEN encounter_count < 500 THEN 'Medium Volume'
        WHEN encounter_count < 1000 THEN 'High Volume'
        ELSE 'Very High Volume'
    END AS volume_tier,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM providers
