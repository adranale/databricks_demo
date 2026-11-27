-- GOLD LAYER: plate origin (local vs. out-of-town)
CREATE OR REFRESH MATERIALIZED VIEW opendata.gold.strafzettel_by_plate_origin
COMMENT 'Ticket count, share, and weekend ratio per license-plate origin group'
AS
WITH per_origin AS (
  SELECT
    plate_origin,
    count(*) AS tickets,
    round(avg(CASE WHEN is_weekend THEN 100.0 ELSE 0.0 END), 1) AS weekend_share_pct
  FROM opendata.silver.strafzettel_cleaned
  GROUP BY plate_origin
)
SELECT
  plate_origin,
  tickets,
  round(tickets * 100.0 / sum(tickets) OVER (), 2) AS share_pct,
  weekend_share_pct
FROM per_origin