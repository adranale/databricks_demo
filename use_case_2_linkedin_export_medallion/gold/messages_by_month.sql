-- GOLD: messaging activity by month
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.messages_by_month
COMMENT 'Messages per month split into own vs received, with active conversation count'
AS
WITH per_month AS (
  SELECT
    date_trunc('month', sent_at) AS month,
    sum(CASE WHEN is_own_message THEN 1 ELSE 0 END) AS own_messages,
    sum(CASE WHEN is_own_message THEN 0 ELSE 1 END) AS received_messages,
    count(*) AS total_messages,
    count(DISTINCT conversation_id) AS active_conversations
  FROM personal.silver.messages_cleaned
  GROUP BY ALL
)
SELECT
  month,
  own_messages,
  received_messages,
  total_messages,
  active_conversations,
  round(own_messages * 100.0 / total_messages, 1) AS own_share_pct
FROM per_month