-- SILVER: cleaned inferences about the account holder
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.inferences_cleaned
COMMENT 'One row per inferred attribute: category, type, description, inferred value'
AS
SELECT DISTINCT
  nullif(trim(category), '') AS category,
  nullif(trim(inference_type), '') AS inference_type,
  nullif(trim(description), '') AS description,
  nullif(trim(inference), '') AS inference
FROM personal.bronze.inferences_raw
WHERE trim(category) <> '' AND trim(inference_type) <> ''