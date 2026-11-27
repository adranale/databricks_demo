-- SILVER: cleaned positions (handles 'Jan 2025' and bare-year '2025' date formats)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.positions_cleaned
COMMENT 'One row per position: company, title, location, parsed start/end, current-role flag'
AS
SELECT DISTINCT
  trim(company_name) AS company_name,
  trim(title) AS title,
  nullif(trim(description), '') AS description,
  nullif(trim(location), '') AS location,
  date(coalesce(try_to_timestamp(started_on_string, 'MMM yyyy'), try_to_timestamp(started_on_string, 'yyyy'))) AS started_on,
  date(coalesce(try_to_timestamp(finished_on_string, 'MMM yyyy'), try_to_timestamp(finished_on_string, 'yyyy'))) AS finished_on,
  finished_on_string = '' OR finished_on_string IS NULL AS is_current
FROM personal.bronze.positions_raw
WHERE coalesce(try_to_timestamp(started_on_string, 'MMM yyyy'), try_to_timestamp(started_on_string, 'yyyy')) IS NOT NULL