-- Fact table for telegram messages
-- Links messages to dimension tables

WITH messages AS (
    SELECT * FROM stg_telegram_messages
),

channels AS (
    SELECT * FROM dim_channels
),

dates AS (
    SELECT * FROM dim_dates
)

SELECT
    -- Fact identifiers
    m.message_id,
    m.channel_name,
    
    -- Foreign keys
    c.channel_key,
    d.date_key,
    
    -- Message attributes
    m.message_text,
    m.message_length,
    m.view_count,
    m.forward_count,
    m.has_image,
    
    -- Timestamps
    m.message_datetime
    
FROM messages m
LEFT JOIN channels c ON m.channel_name = c.channel_name
LEFT JOIN dates d ON m.message_date = d.full_date
