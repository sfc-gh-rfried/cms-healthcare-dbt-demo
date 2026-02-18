WITH source AS (
    SELECT * FROM {{ source('medicare', 'PROVIDERS') }}
),

staged AS (
    SELECT
        PROVIDER_ID AS provider_id,
        ORGANIZATION_ID AS organization_id,
        NAME AS provider_name,
        GENDER AS gender,
        SPECIALITY AS specialty,
        ADDRESS AS street_address,
        CITY AS city,
        STATE AS state,
        ZIP AS zip_code,
        LAT AS latitude,
        LON AS longitude,
        ENCOUNTERS AS encounter_count,
        PROCEDURES AS procedure_count,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)

SELECT * FROM staged
