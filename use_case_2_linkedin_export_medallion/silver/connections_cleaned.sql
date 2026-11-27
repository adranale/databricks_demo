-- SILVER: cleaned connections with calendar dimensions
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.connections_cleaned
COMMENT 'One row per connection: parsed connect date, name, company, position, profile URL'
AS
WITH typed AS (
  SELECT
    try_to_timestamp(connected_on, 'dd MMM yyyy') AS connected_on,
    trim(first_name) AS first_name,
    trim(last_name) AS last_name,
    trim(profile_url) AS profile_url,
    nullif(trim(email_address), '') AS email,
    nullif(trim(company), '') AS company,
    nullif(trim(position), '') AS position,
    _metadata.file_name AS source_file
  FROM personal.bronze.connections_raw
)
SELECT DISTINCT
  connected_on,
  date(connected_on) AS connected_date,
  year(connected_on) AS connected_year,
  month(connected_on) AS connected_month,
  first_name,
  last_name,
  profile_url,
  email,
  company,
  position,
  source_file
FROM typed
WHERE connected_on IS NOT NULL