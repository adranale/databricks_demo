-- GOLD: network growth per year with cumulative total
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.connections_growth
COMMENT 'New connections and cumulative network size per year'
AS
WITH per_year AS (
  SELECT
    connected_year,
    count(*) AS connections
  FROM personal.silver.connections_cleaned
  GROUP BY connected_year
)
SELECT
  connected_year,
  connections,
  round(connections * 100.0 / sum(connections) OVER (), 2) AS share_pct,
  sum(connections) OVER (ORDER BY connected_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_connections
FROM per_year