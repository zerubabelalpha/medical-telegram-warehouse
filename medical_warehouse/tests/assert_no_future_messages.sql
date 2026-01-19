-- Custom test: Ensure no messages have future dates
-- This test should return 0 rows to pass

SELECT
    message_id,
    channel_name,
    message_datetime
FROM {{ ref('fct_messages') }}
WHERE message_datetime > CURRENT_TIMESTAMP
