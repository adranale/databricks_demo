-- GOLD: members followed per month with cumulative total
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.member_follows_by_month
COMMENT 'Members followed per month with cumulative total'
AS
WITH per_month AS (
  SELECT
    date_trunc('month', followed_at) AS month,
    count(*) AS new_follows
  FROM personal.silver.member_follows_cleaned
  GROUP BY ALL
)
SELECT
  month,
  new_follows,
  sum(new_follows) OVER (ORDER BY month) AS total_follows
FROM per_month