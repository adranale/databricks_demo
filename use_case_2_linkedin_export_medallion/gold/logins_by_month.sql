-- GOLD: logins per month (platform activity)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.logins_by_month
COMMENT 'Logins per month'
AS
SELECT
  date_trunc('month', login_at) AS month,
  count(*) AS logins
FROM personal.silver.logins_cleaned
GROUP BY ALL