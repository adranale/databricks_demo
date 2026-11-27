-- SILVER: cleaned profile languages
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.languages_cleaned
COMMENT 'One row per language with optional proficiency'
AS
SELECT DISTINCT
  nullif(trim(language_name), '') AS language_name,
  nullif(trim(proficiency), '') AS proficiency
FROM personal.bronze.languages_raw
WHERE trim(language_name) <> ''