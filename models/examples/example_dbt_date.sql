{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2024-12-31' as date)"
    ) }}
)

SELECT
    date_day,
    {{ dbt_date.day_of_week('date_day') }} AS day_of_week,
    {{ dbt_date.day_name('date_day') }} AS day_name,
    {{ dbt_date.day_of_month('date_day') }} AS day_of_month,
    {{ dbt_date.week_of_year('date_day') }} AS week_of_year,
    {{ dbt_date.month_name('date_day') }} AS month_name,
    MONTH(date_day) AS month_number,
    QUARTER(date_day) AS quarter_number,
    YEAR(date_day) AS year_number
FROM date_spine
