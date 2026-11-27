-- SILVER: cleaned security challenges (parsed timestamp, derived device; IP dropped for privacy)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.security_challenges_cleaned
COMMENT 'One row per security challenge: parsed timestamp, device, country, challenge type'
AS
WITH typed AS (
  SELECT
    -- Java-style 'EEE MMM dd HH:mm:ss zzz yyyy' timestamp: strip the day-of-week first
    try_to_timestamp(regexp_replace(challenge_date_string, '^[A-Za-z]{3} ', ''), 'MMM dd HH:mm:ss zzz yyyy') AS challenged_at,
    trim(user_agent) AS user_agent,
    nullif(trim(country), '') AS country,
    nullif(trim(challenge_type), '') AS challenge_type
  FROM personal.bronze.security_challenges_raw
)
SELECT DISTINCT
  challenged_at,
  date(challenged_at) AS challenged_date,
  CASE
    WHEN user_agent LIKE '%iPhone%' THEN 'iPhone'
    WHEN user_agent LIKE '%iPad%' THEN 'iPad'
    WHEN user_agent LIKE '%Macintosh%' THEN 'Mac'
    WHEN user_agent LIKE '%Android%' THEN 'Android'
    WHEN user_agent LIKE '%Windows%' THEN 'Windows'
    ELSE 'Other'
  END AS device,
  country,
  challenge_type
FROM typed
WHERE challenged_at IS NOT NULL