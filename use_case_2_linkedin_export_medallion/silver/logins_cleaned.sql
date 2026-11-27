-- SILVER: cleaned logins (parsed timestamp, derived device; IP dropped for privacy)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.logins_cleaned
COMMENT 'One row per login: parsed timestamp, device/browser derived from the user agent, login type'
AS
WITH typed AS (
  SELECT
    -- Java-style 'EEE MMM dd HH:mm:ss zzz yyyy' timestamp: Spark rejects the
    -- day-of-week letter (E), so strip the leading 'Thu '/'Mon '/... first.
    try_to_timestamp(regexp_replace(login_date_string, '^[A-Za-z]{3} ', ''), 'MMM dd HH:mm:ss zzz yyyy') AS login_at,
    trim(user_agent) AS user_agent,
    nullif(trim(login_type), '') AS login_type
  FROM personal.bronze.logins_raw
)
SELECT DISTINCT
  login_at,
  date(login_at) AS login_date,
  CASE
    WHEN user_agent LIKE '%iPhone%' THEN 'iPhone'
    WHEN user_agent LIKE '%iPad%' THEN 'iPad'
    WHEN user_agent LIKE '%Macintosh%' THEN 'Mac'
    WHEN user_agent LIKE '%Android%' THEN 'Android'
    WHEN user_agent LIKE '%Windows%' THEN 'Windows'
    ELSE 'Other'
  END AS device,
  CASE
    WHEN user_agent LIKE '%Edg/%' THEN 'Edge'
    WHEN user_agent LIKE '%Firefox/%' THEN 'Firefox'
    WHEN user_agent LIKE '%Chrome/%' THEN 'Chrome'
    WHEN user_agent LIKE '%Safari/%' THEN 'Safari'
    ELSE 'Other'
  END AS browser,
  login_type
FROM typed
WHERE login_at IS NOT NULL