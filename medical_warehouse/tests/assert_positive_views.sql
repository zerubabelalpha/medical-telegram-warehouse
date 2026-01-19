-- Custom test: Ensure view counts are non-negative
-- This test should return 0 rows to pass

SELECT
    message_id,
    channel_name,
    view_count
FROM {{ ref('fct_messages') }}
WHERE view_count < 0
