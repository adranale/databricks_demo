-- GOLD: ad clicks per month (how LinkedIn advertising engages the account)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.ads_clicked_by_month
COMMENT 'Ad clicks per month'
AS
SELECT
  date_trunc('month', clicked_at) AS month,
  count(*) AS clicks
FROM personal.silver.ads_clicked_cleaned
GROUP BY ALL