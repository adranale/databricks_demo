-- SILVER: cleaned certifications (name, authority, parsed dates)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.certifications_cleaned
COMMENT 'One row per certification: name, authority, parsed start/finish dates'
AS
SELECT DISTINCT
  trim(certification_name) AS certification_name,
  nullif(trim(authority), '') AS authority,
  try_to_timestamp(started_on_string, 'MMM yyyy') AS started_on,
  try_to_timestamp(finished_on_string, 'MMM yyyy') AS finished_on
FROM personal.bronze.certifications_raw
WHERE trim(certification_name) <> ''