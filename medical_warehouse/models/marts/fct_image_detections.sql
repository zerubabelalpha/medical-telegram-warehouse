-- Fact table for image detections
-- Joins YOLO detections with messages to get channel and date keys

WITH detections AS (
    SELECT * FROM {{ ref('stg_yolo_detections') }}
),

messages AS (
    SELECT * FROM {{ ref('fct_messages') }}
)

SELECT
    d.message_key,
    d.message_id,
    m.channel_key,
    m.date_key,
    d.detected_class,
    d.confidence_score,
    d.image_category
FROM detections d
JOIN messages m ON d.message_key = m.message_key
