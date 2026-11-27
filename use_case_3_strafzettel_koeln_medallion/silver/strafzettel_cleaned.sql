-- SILVER LAYER
-- Cleaned, typed, deduplicated ticket facts.
-- * Parses the ticket timestamp, derives calendar dimensions (date, month, hour, weekday, weekend)
-- * Normalizes text attributes (trim + upper) and nulls out empty violation codes
-- * Classifies plate origin: Köln (K), other German district, foreign/other
-- * Drops exact duplicate records (~1% of the raw file) and rows without timestamp or violation code
CREATE OR REFRESH MATERIALIZED VIEW opendata.silver.strafzettel_cleaned
COMMENT 'One row per distinct ticket: parsed timestamp, normalized attributes, derived time and plate-origin dimensions'
AS
WITH typed AS (
  SELECT
    try_to_timestamp(datum_von) AS ticket_timestamp,
    upper(trim(kennzeichen1)) AS plate_prefix,
    upper(trim(fahrzeugart)) AS vehicle_type,
    upper(trim(fabrikat)) AS vehicle_brand,
    upper(trim(strasse)) AS street,
    trim(hausnummer) AS house_number,
    upper(trim(tatbestand1)) AS violation_code_1,
    nullif(upper(trim(tatbestand2)), '') AS violation_code_2,
    nullif(upper(trim(tatbestand3)), '') AS violation_code_3,
    _metadata.file_name       AS source_file
  FROM opendata.bronze.strafzettel_raw
)
SELECT DISTINCT
  ticket_timestamp,
  date(ticket_timestamp) AS ticket_date,
  month(ticket_timestamp) AS ticket_month,
  date_format(ticket_timestamp, 'MMM') AS ticket_month_name,
  hour(ticket_timestamp) AS ticket_hour,
  ((dayofweek(ticket_timestamp) + 5) % 7) + 1 AS weekday_num,
  CASE ((dayofweek(ticket_timestamp) + 5) % 7) + 1
    WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue' WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu'
    WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat' ELSE 'Sun'
  END AS weekday_name,
  ((dayofweek(ticket_timestamp) + 5) % 7) + 1 >= 6 AS is_weekend,
  CASE
    WHEN plate_prefix IS NULL THEN 'Unknown'
    WHEN plate_prefix = 'K' THEN 'Köln (K)'
    WHEN plate_prefix RLIKE '^[A-ZÄÖÜ]{1,3}$' THEN 'Other German district'
    ELSE 'Foreign / other'
  END AS plate_origin,
  plate_prefix,
  vehicle_type,
  vehicle_brand,
  street,
  house_number,
  violation_code_1,
  violation_code_2,
  violation_code_3,
  source_file
FROM typed
WHERE ticket_timestamp IS NOT NULL
  AND violation_code_1 IS NOT NULL