-- GOLD: security challenges by type (account security hygiene)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.security_challenges_summary
COMMENT 'Security challenges by type with counts, first/last seen and affected countries'
AS
SELECT
  challenge_type,
  count(*) AS challenges,
  min(challenged_date) AS first_seen,
  max(challenged_date) AS last_seen,
  count(DISTINCT country) AS countries
FROM personal.silver.security_challenges_cleaned
WHERE challenge_type IS NOT NULL
GROUP BY ALL