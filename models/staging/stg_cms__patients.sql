WITH source AS (
    SELECT * FROM {{ ref('raw_patients') }}
),

staged AS (
    SELECT
        PATIENT_ID AS patient_id,
        BIRTHDATE AS birth_date,
        DEATHDATE AS death_date,
        FIRST AS first_name,
        LAST AS last_name,
        GENDER AS gender,
        RACE AS race,
        ETHNICITY AS ethnicity,
        CITY AS city,
        STATE AS state,
        COUNTY AS county,
        ZIP AS zip_code,
        HEALTHCARE_EXPENSES AS lifetime_healthcare_expenses,
        HEALTHCARE_COVERAGE AS lifetime_healthcare_coverage,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)

SELECT * FROM staged
