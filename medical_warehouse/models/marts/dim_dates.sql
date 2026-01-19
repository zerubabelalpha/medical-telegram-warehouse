-- Date dimension table for time-based analysis
-- Generates a complete date dimension from the earliest to latest message dates

{{ config(materialized='table') }}

WITH date_range AS (
    SELECT
        generate_series(
            '2022-01-01'::date,
            '2026-12-31'::date,
            '1 day'::interval
        )::date AS date_day
),

date_details AS (
    SELECT
        date_day AS full_date,
        MD5(date_day::text)::uuid AS date_key,
        EXTRACT(DAY FROM date_day)::integer AS day_of_month,
        EXTRACT(DOW FROM date_day)::integer AS day_of_week,
        TO_CHAR(date_day, 'Day') AS day_name,
        EXTRACT(WEEK FROM date_day)::integer AS week_of_year,
        EXTRACT(MONTH FROM date_day)::integer AS month,
        TO_CHAR(date_day, 'Month') AS month_name,
        EXTRACT(QUARTER FROM date_day)::integer AS quarter,
        EXTRACT(YEAR FROM date_day)::integer AS year,
        CASE 
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM date_range
)

SELECT * FROM date_details
