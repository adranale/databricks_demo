-- SILVER: cleaned skills list
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.skills_cleaned
COMMENT 'One row per listed skill'
AS
SELECT DISTINCT trim(skill_name) AS skill
FROM personal.bronze.skills_raw
WHERE trim(skill_name) IS NOT NULL