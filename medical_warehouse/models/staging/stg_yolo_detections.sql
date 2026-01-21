-- Staging model for YOLO detections
-- Standardizes the raw yolo detection data

WITH source AS (
    SELECT * FROM {{ source('raw', 'yolo_detections') }}
),

cleaned AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['message_id', 'channel_name']) }} AS message_key,
        message_id,
        channel_name,
        detected_class,
        CAST(confidence_score AS FLOAT) AS confidence_score,
        image_category,
        loaded_at
    FROM source
    WHERE message_id IS NOT NULL
)

SELECT * FROM cleaned
