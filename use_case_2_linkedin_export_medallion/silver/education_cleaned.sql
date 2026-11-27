-- SILVER: cleaned education history (school, degree, years)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.education_cleaned
COMMENT 'One row per school: name, degree, start/end years'
AS
SELECT DISTINCT
  trim(school_name) AS school_name,
  nullif(trim(degree_name), '') AS degree_name,
  try_cast(start_date_string AS INT) AS start_year,
  try_cast(end_date_string AS INT) AS end_year
FROM personal.bronze.education_raw
WHERE trim(school_name) <> ''