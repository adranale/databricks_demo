-- SILVER: cleaned registration (account creation timestamp)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.registration_cleaned
COMMENT 'One row with the parsed account registration timestamp'
AS
WITH typed AS (
  SELECT
    -- 'M/d/yy, h:mm a' with single-digit month/day/hour
    try_to_timestamp(registered_at_string, 'M/d/yy, h:mm a') AS registered_at
  FROM personal.bronze.registration_raw
)
SELECT DISTINCT
  registered_at,
  date(registered_at) AS registered_date,
  year(registered_at) AS registered_year
FROM typed
WHERE registered_at IS NOT NULL