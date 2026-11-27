-- BRONZE: LinkedIn search queries (people, jobs, topics)
-- NOTE: the source CSV has a few malformed rows (an extra comma in the query text);
-- rows where the query text cannot be parsed are dropped in silver.
CREATE OR REFRESH STREAMING TABLE personal.bronze.search_queries_raw
COMMENT 'Raw LinkedIn search queries, one row per search (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Time` AS searched_at_string,
  `Search Query` AS search_query,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'SearchQueries.csv'
)