-- GOLD: LinkedIn Learning summary (single row)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.learning_summary
COMMENT 'Learning content items, completed and saved counts, latest activity'
AS
SELECT
  count(*) AS content_items,
  sum(CASE WHEN is_completed THEN 1 ELSE 0 END) AS completed,
  sum(CASE WHEN is_saved THEN 1 ELSE 0 END) AS saved,
  max(coalesce(completed_at, last_watched_at)) AS last_activity
FROM personal.silver.learning_cleaned