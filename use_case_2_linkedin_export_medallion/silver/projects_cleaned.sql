-- SILVER: cleaned profile projects
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.projects_cleaned
COMMENT 'One row per project: title, description, url and start/finish as raw strings'
AS
SELECT DISTINCT
  nullif(trim(title), '') AS title,
  nullif(trim(description), '') AS description,
  nullif(trim(url), '') AS url,
  nullif(trim(started_on_string), '') AS started_on,
  nullif(trim(finished_on_string), '') AS finished_on
FROM personal.bronze.projects_raw
WHERE trim(title) <> ''