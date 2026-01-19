-- Dimension table for Telegram channels
-- Contains channel metadata and aggregated metrics

WITH channel_stats AS (
    SELECT
        channel_name,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        COUNT(*) AS total_posts,
        ROUND(AVG(view_count), 2) AS avg_views
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
),

channel_classification AS (
    SELECT
        channel_name,
        CASE
            WHEN channel_name IN ('CheMed123', 'tikvahpharma') THEN 'Pharmaceutical'
            WHEN channel_name = 'lobelia4cosmetics' THEN 'Cosmetics'
            ELSE 'Medical'
        END AS channel_type
    FROM channel_stats
)

SELECT
    MD5(cs.channel_name)::uuid AS channel_key,
    cs.channel_name,
    cc.channel_type,
    cs.first_post_date,
    cs.last_post_date,
    cs.total_posts,
    cs.avg_views
FROM channel_stats cs
LEFT JOIN channel_classification cc ON cs.channel_name = cc.channel_name
