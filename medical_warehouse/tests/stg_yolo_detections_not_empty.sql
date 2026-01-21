-- Test fails if staging yolo detections table has zero rows
SELECT *
FROM (
  SELECT COUNT(*) AS cnt FROM {{ ref('stg_yolo_detections') }}
) t
WHERE t.cnt = 0
