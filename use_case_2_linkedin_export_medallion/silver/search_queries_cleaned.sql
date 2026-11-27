-- SILVER: cleaned search queries (parsed timestamp, raw query, normalized term)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.search_queries_cleaned
COMMENT 'One row per search: parsed timestamp, raw query text and normalized search term'
AS
WITH typed AS (
  SELECT
    -- 'yyyy/MM/dd HH:mm:ss UTC' -> parse the local part only
    try_to_timestamp(substring(searched_at_string, 1, 19), 'yyyy/MM/dd HH:mm:ss') AS searched_at,
    trim(search_query) AS search_query
  FROM personal.bronze.search_queries_raw
)
SELECT DISTINCT
  searched_at,
  date(searched_at) AS searched_date,
  search_query,
  lower(search_query) AS search_term
FROM typed
WHERE searched_at IS NOT NULL AND search_query <> ''