-- SILVER: cleaned followed members (parsed timestamp)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.member_follows_cleaned
COMMENT 'One row per followed member: parsed follow timestamp, status, name'
AS
WITH typed AS (
  SELECT
    -- 'yyyy-MM-dd HH:mm:ss'
    try_to_timestamp(followed_at_string, 'yyyy-MM-dd HH:mm:ss') AS followed_at,
    nullif(trim(status), '') AS status,
    nullif(trim(full_name), '') AS full_name
  FROM personal.bronze.member_follows_raw
)
SELECT DISTINCT
  followed_at,
  date(followed_at) AS followed_date,
  status,
  full_name
FROM typed
WHERE followed_at IS NOT NULL