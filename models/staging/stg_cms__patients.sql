WITH source AS (
    SELECT * FROM {{ source('medicare', 'PATIENTS') }}
),

staged AS (
    SELECT
        PATIENT_ID AS patient_id,
        BIRTHDATE AS birth_date,
        DEATHDATE AS death_date,
        SSN AS ssn_hash,
        DRIVERS AS drivers_license,
        PASSPORT AS passport_number,
        PREFIX AS name_prefix,
        FIRST AS first_name,
        LAST AS last_name,
        SUFFIX AS name_suffix,
        MAIDEN AS maiden_name,
        MARITAL AS marital_status,
        RACE AS race,
        ETHNICITY AS ethnicity,
        GENDER AS gender,
        BIRTHPLACE AS birth_place,
        ADDRESS AS street_address,
        CITY AS city,
        STATE AS state,
        COUNTY AS county,
        ZIP AS zip_code,
        LAT AS latitude,
        LON AS longitude,
        HEALTHCARE_EXPENSES AS lifetime_healthcare_expenses,
        HEALTHCARE_COVERAGE AS lifetime_healthcare_coverage,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)

SELECT * FROM staged
