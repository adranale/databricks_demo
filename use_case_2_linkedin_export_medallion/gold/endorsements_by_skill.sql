-- GOLD: accepted endorsements per skill (credibility ranking)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.endorsements_by_skill
COMMENT 'Accepted endorsements per skill: counts, distinct endorsers, latest endorsement, share'
AS
WITH per_skill AS (
  SELECT
    skill_name,
    count(*) AS endorsements,
    count(DISTINCT endorser_name) AS distinct_endorsers,
    max(endorsement_at) AS latest_endorsement
  FROM personal.silver.endorsements_received_cleaned
  WHERE endorsement_status = 'ACCEPTED'
  GROUP BY skill_name
)
SELECT
  skill_name,
  endorsements,
  distinct_endorsers,
  latest_endorsement,
  round(endorsements * 100.0 / sum(endorsements) OVER (), 2) AS share_pct
FROM per_skill