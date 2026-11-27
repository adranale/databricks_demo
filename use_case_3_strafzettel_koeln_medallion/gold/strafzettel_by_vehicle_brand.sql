-- GOLD LAYER: vehicle mix by brand and type
CREATE OR REFRESH MATERIALIZED VIEW opendata.gold.strafzettel_by_vehicle_brand
COMMENT 'Ticket count and share per vehicle brand and vehicle type'
AS
WITH per_brand AS (
  SELECT
    vehicle_brand,
    vehicle_type,
    count(*) AS tickets
  FROM opendata.silver.strafzettel_cleaned
  WHERE vehicle_brand IS NOT NULL
  GROUP BY vehicle_brand, vehicle_type
)
SELECT
  vehicle_brand,
  vehicle_type,
  tickets,
  round(tickets * 100.0 / sum(tickets) OVER (), 2) AS share_pct
FROM per_brand