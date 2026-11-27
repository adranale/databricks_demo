-- GOLD: network composition by job title
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.connections_by_position
COMMENT 'Connection count and share per position title'
AS
WITH per_position AS (
  SELECT
    position,
    count(*) AS connections
  FROM personal.silver.connections_cleaned
  WHERE position IS NOT NULL
  GROUP BY position
)
SELECT
  position,
  connections,
  round(connections * 100.0 / sum(connections) OVER (), 2) AS share_pct
FROM per_position