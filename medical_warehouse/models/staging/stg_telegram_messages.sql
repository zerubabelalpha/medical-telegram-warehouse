-- Staging model for telegram messages
-- Cleans and standardizes raw telegram message data

WITH source AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
),

cleaned AS (
    SELECT
        -- Primary keys
        message_id,
        channel_name,
        
        -- Dates
        CAST(message_date AS TIMESTAMP) AS message_datetime,
        DATE(message_date) AS message_date,
        
        -- Message content
        COALESCE(message_text, '') AS message_text,
        LENGTH(COALESCE(message_text, '')) AS message_length,
        
        -- Media
        has_media,
        CASE 
            WHEN image_path IS NOT NULL AND image_path != '' THEN TRUE
            ELSE FALSE
        END AS has_image,
        image_path,
        
        -- Engagement metrics
        COALESCE(views, 0) AS view_count,
        COALESCE(forwards, 0) AS forward_count,
        
        -- Metadata
        loaded_at
        
    FROM source
    
    -- Filter out invalid records
    WHERE message_id IS NOT NULL
      AND channel_name IS NOT NULL
      AND message_date IS NOT NULL
      -- Filter out empty messages without media
      AND (
          (message_text IS NOT NULL AND message_text != '')
          OR has_media = TRUE
      )
)

SELECT * FROM cleaned
