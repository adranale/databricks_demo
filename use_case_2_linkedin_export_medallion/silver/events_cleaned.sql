-- SILVER: cleaned events (parsed start time)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.events_cleaned
COMMENT 'One row per event: name, parsed start time, status, external url'
AS
WITH typed AS (
  SELECT
    nullif(trim(event_name), '') AS event_name,
    -- 'MMM d, yyyy hh:mm a - MMM d, yyyy hh:mm a' -> parse the start part only
    try_to_timestamp(split_part(event_time_string, ' - ', 1), 'MMM d, yyyy hh:mm a') AS event_start,
    nullif(trim(status), '') AS status,
    nullif(trim(external_url), '') AS external_url
  FROM personal.bronze.events_raw
)
SELECT DISTINCT
  event_name,
  event_start,
  date(event_start) AS event_date,
  status,
  external_url
FROM typed
WHERE event_name IS NOT NULL