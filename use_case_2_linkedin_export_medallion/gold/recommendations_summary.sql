-- GOLD: recommendations given vs received (single row)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.recommendations_summary
COMMENT 'Recommendations given and received: totals, visible share, latest activity'
AS
WITH given AS (
  SELECT
    count(*) AS given_total,
    sum(CASE WHEN is_visible THEN 1 ELSE 0 END) AS given_visible,
    max(created_at) AS last_given
  FROM personal.silver.recommendations_given_cleaned
),
recv AS (
  SELECT
    count(*) AS received_total,
    sum(CASE WHEN is_visible THEN 1 ELSE 0 END) AS received_visible,
    max(created_at) AS last_received
  FROM personal.silver.recommendations_received_cleaned
)
SELECT
  given.*,
  recv.*
FROM given, recv