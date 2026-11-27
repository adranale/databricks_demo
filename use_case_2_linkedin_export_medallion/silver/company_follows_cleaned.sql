-- SILVER: cleaned company follows (Java-style timestamps: 'Mon Nov 23 09:07:03 GMT 2026')
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.company_follows_cleaned
COMMENT 'One row per followed organization with parsed follow timestamp'
AS
WITH typed AS (
  SELECT
    trim(organization) AS organization,
    -- Spark's datetime formatter rejects the day-of-week letter (E), so strip the leading
    -- 'Mon '/'Tue '/... from the Java-style timestamp before parsing.
    try_to_timestamp(regexp_replace(followed_on_string, '^[A-Za-z]{3} ', ''), 'MMM dd HH:mm:ss zzz yyyy') AS followed_at
  FROM personal.bronze.company_follows_raw
)
SELECT DISTINCT
  organization,
  followed_at,
  date(followed_at) AS followed_date,
  year(followed_at) AS followed_year
FROM typed
WHERE followed_at IS NOT NULL