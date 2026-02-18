WITH source AS (
    SELECT * FROM {{ source('medicare', 'CLAIMS') }}
),

staged AS (
    SELECT
        CLAIM_ID AS claim_id,
        PATIENT_ID AS patient_id,
        PROVIDER_ID AS provider_id,
        PRIMARY_PATIENT_INSURANCE_ID AS primary_insurance_id,
        SECONDARY_PATIENT_INSURANCE_ID AS secondary_insurance_id,
        DEPARTMENTID AS department_id,
        PATIENTDEPARTMENTID AS patient_department_id,
        DIAGNOSIS1 AS diagnosis_code_1,
        DIAGNOSIS2 AS diagnosis_code_2,
        DIAGNOSIS3 AS diagnosis_code_3,
        DIAGNOSIS4 AS diagnosis_code_4,
        DIAGNOSIS5 AS diagnosis_code_5,
        DIAGNOSIS6 AS diagnosis_code_6,
        DIAGNOSIS7 AS diagnosis_code_7,
        DIAGNOSIS8 AS diagnosis_code_8,
        REFERRING_PROVIDER_ID AS referring_provider_id,
        ENCOUNTER_ID AS encounter_id,
        CURRENTILLNESSDATE AS current_illness_date,
        SERVICEDATE AS service_date,
        SUPERVISING_PROVIDER_ID AS supervising_provider_id,
        STATUS1 AS claim_status_1,
        STATUS2 AS claim_status_2,
        STATUSP AS claim_status_p,
        OUTSTANDING1 AS outstanding_amount_1,
        OUTSTANDING2 AS outstanding_amount_2,
        OUTSTANDINGP AS outstanding_amount_p,
        LASTBILLEDDATE1 AS last_billed_date_1,
        LASTBILLEDDATE2 AS last_billed_date_2,
        LASTBILLEDDATEP AS last_billed_date_p,
        HEALTHCARECLAIMTYPEID1 AS claim_type_id_1,
        HEALTHCARECLAIMTYPEID2 AS claim_type_id_2,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)

SELECT * FROM staged
