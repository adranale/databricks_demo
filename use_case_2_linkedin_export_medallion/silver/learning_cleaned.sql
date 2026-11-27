-- SILVER: cleaned LinkedIn Learning history (parsed timestamps, completion and saved flags)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.learning_cleaned
COMMENT 'One row per learning content item: title, type, parsed watch/completion timestamps, flags'
AS
WITH typed AS (
  SELECT
    trim(content_title) AS content_title,
    trim(content_type) AS content_type,
    -- 'yyyy-MM-dd HH:mm UTC' -> parse the local part only
    try_to_timestamp(substring(last_watched_at_string, 1, 16), 'yyyy-MM-dd HH:mm') AS last_watched_at,
    try_to_timestamp(substring(completed_at_string, 1, 16), 'yyyy-MM-dd HH:mm') AS completed_at,
    lower(trim(content_saved)) = 'true' AS is_saved
  FROM personal.bronze.learning_raw
)
SELECT DISTINCT
  content_title,
  content_type,
  last_watched_at,
  completed_at,
  completed_at IS NOT NULL AS is_completed,
  is_saved
FROM typed
WHERE content_title <> ''