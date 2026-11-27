-- SILVER: cleaned publications (parsed publish date)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.publications_cleaned
COMMENT 'One row per publication: name, parsed publish date, publisher, url'
AS
WITH typed AS (
  SELECT
    nullif(trim(publication_name), '') AS publication_name,
    -- 'MMM d, yyyy' (single-digit day)
    try_to_timestamp(published_on_string, 'MMM d, yyyy') AS published_on,
    nullif(trim(publisher), '') AS publisher,
    nullif(trim(url), '') AS url
  FROM personal.bronze.publications_raw
)
SELECT DISTINCT
  publication_name,
  published_on,
  date(published_on) AS published_date,
  publisher,
  url
FROM typed
WHERE publication_name IS NOT NULL