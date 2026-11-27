-- SILVER: cleaned ads clicked (parsed timestamp, ad identifier)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.ads_clicked_cleaned
COMMENT 'One row per ad click: parsed timestamp and ad identifier'
AS
WITH typed AS (
  SELECT
    -- 'yyyy/MM/dd HH:mm:ss UTC' -> parse the local part only
    try_to_timestamp(substring(clicked_at_string, 1, 19), 'yyyy/MM/dd HH:mm:ss') AS clicked_at,
    trim(ad_id) AS ad_id
  FROM personal.bronze.ads_clicked_raw
)
SELECT DISTINCT
  clicked_at,
  date(clicked_at) AS clicked_date,
  ad_id
FROM typed
WHERE clicked_at IS NOT NULL