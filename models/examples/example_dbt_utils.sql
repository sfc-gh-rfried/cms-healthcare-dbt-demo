WITH claims AS (
    SELECT * FROM {{ ref('stg_cms__claims') }}
    WHERE claim_id IS NOT NULL
    LIMIT 1000
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['claim_id', 'patient_id']) }} AS claim_patient_key,
    claim_id,
    patient_id,
    service_date,
    {{ dbt_utils.star(from=ref('stg_cms__claims'), except=['claim_id', 'patient_id', 'service_date', '_loaded_at']) }}
FROM claims
