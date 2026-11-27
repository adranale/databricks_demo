-- GOLD: headline endorsement KPIs (single row)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.endorsements_summary
COMMENT 'Endorsements received/given, accepted/rejected split and top skill'
AS
WITH recv AS (
  SELECT
    count(*) AS received_total,
    sum(CASE WHEN endorsement_status = 'ACCEPTED' THEN 1 ELSE 0 END) AS accepted,
    sum(CASE WHEN endorsement_status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected
  FROM personal.silver.endorsements_received_cleaned
),
given AS (
  SELECT count(*) AS given_total
  FROM personal.silver.endorsements_given_cleaned
),
top_skill AS (
  SELECT max(struct(endorsements, skill_name)) AS s
  FROM personal.gold.endorsements_by_skill
)
SELECT
  recv.received_total,
  recv.accepted,
  recv.rejected,
  given.given_total,
  top_skill.s.skill_name AS top_skill,
  top_skill.s.endorsements AS top_skill_endorsements
FROM recv, given, top_skill