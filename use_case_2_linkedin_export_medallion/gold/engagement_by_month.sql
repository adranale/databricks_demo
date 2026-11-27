-- GOLD: feed engagement per month and event type
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.engagement_by_month
COMMENT 'Engagement events per month and event type (reaction, comment, share, repost, vote)'
AS
SELECT
  date_trunc('month', occurred_at) AS month,
  event_type,
  count(*) AS events
FROM personal.silver.engagement_cleaned
GROUP BY ALL