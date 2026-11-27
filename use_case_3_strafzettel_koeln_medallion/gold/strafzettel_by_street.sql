-- GOLD LAYER: ticket hot spots by street
CREATE OR REFRESH MATERIALIZED VIEW opendata.gold.strafzettel_by_street
COMMENT 'Ticket count, share of all tickets, and local-plate ratio per street'
AS
WITH per_street AS (
  SELECT
    street,
    count(*) AS tickets,
    round(avg(CASE WHEN plate_origin = 'Köln (K)' THEN 100.0 ELSE 0.0 END), 1) AS local_share_pct
  FROM opendata.silver.strafzettel_cleaned
  GROUP BY street
)
SELECT
  street,
  tickets,
  round(tickets * 100.0 / sum(tickets) OVER (), 2) AS share_pct,
  local_share_pct
FROM per_street