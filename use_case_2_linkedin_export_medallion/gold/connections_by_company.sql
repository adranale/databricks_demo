-- GOLD: network composition by employer
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.connections_by_company
COMMENT 'Connection count, share and latest connection per company'
AS
WITH per_company AS (
  SELECT
    company,
    count(*) AS connections,
    max(connected_date) AS latest_connection
  FROM personal.silver.connections_cleaned
  WHERE company IS NOT NULL
  GROUP BY company
)
SELECT
  company,
  connections,
  latest_connection,
  round(connections * 100.0 / sum(connections) OVER (), 2) AS share_pct
FROM per_company