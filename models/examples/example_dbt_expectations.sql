WITH claims AS (
    SELECT 
        claim_id,
        patient_id,
        outstanding_amount_1,
        outstanding_amount_2,
        service_date
    FROM {{ ref('stg_cms__claims') }}
    WHERE claim_id IS NOT NULL
    LIMIT 1000
)

SELECT * FROM claims
