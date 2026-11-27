-- GOLD: normalized search terms (what was searched, how often, when)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.search_terms
COMMENT 'Searches per normalized term with first/last seen dates'
AS
SELECT
  search_term,
  count(*) AS searches,
  min(searched_date) AS first_searched,
  max(searched_date) AS last_searched
FROM personal.silver.search_queries_cleaned
WHERE search_term <> ''
GROUP BY search_term