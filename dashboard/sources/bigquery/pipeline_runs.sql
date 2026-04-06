SELECT
  DATE(started_at) AS run_date,
  pipeline_name,
  status,
  rows_loaded,
  ROUND(duration_seconds, 1) AS duration_seconds,
  CASE WHEN error_message = '' THEN NULL ELSE error_message END AS error_message,
  started_at
FROM `practical-gecko-373320.ops.pipeline_runs`
WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
ORDER BY started_at DESC
