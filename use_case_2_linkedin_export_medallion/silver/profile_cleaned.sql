-- SILVER: cleaned profile snapshot (headline, industry, location, summary)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.profile_cleaned
COMMENT 'One row with the profile headline, industry, geo location and summary'
AS
SELECT DISTINCT
  nullif(trim(headline), '') AS headline,
  nullif(trim(industry), '') AS industry,
  nullif(trim(geo_location), '') AS geo_location,
  nullif(trim(summary), '') AS summary
FROM personal.bronze.profile_raw
WHERE trim(headline) <> '' OR trim(industry) <> '' OR trim(geo_location) <> ''