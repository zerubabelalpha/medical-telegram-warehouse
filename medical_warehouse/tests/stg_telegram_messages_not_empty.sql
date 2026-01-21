-- Test fails if staging table has zero rows
SELECT *
FROM (
  SELECT COUNT(*) AS cnt FROM {{ ref('stg_telegram_messages') }}
) t
WHERE t.cnt = 0
