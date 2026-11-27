-- GOLD: profile snapshot (headline fields + content counts, single row)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.profile_overview
COMMENT 'Profile snapshot: headline, industry, location, member since and content counts'
AS
WITH p AS (
  SELECT
    max(headline) AS headline,
    max(industry) AS industry,
    max(geo_location) AS geo_location
  FROM personal.silver.profile_cleaned
),
r AS (
  SELECT min(registered_date) AS member_since
  FROM personal.silver.registration_cleaned
),
cnt AS (
  SELECT 'positions' AS metric, count(*) AS value FROM personal.silver.positions_cleaned
  UNION ALL SELECT 'skills', count(*) FROM personal.silver.skills_cleaned
  UNION ALL SELECT 'languages', count(*) FROM personal.silver.languages_cleaned
  UNION ALL SELECT 'projects', count(*) FROM personal.silver.projects_cleaned
  UNION ALL SELECT 'publications', count(*) FROM personal.silver.publications_cleaned
  UNION ALL SELECT 'events', count(*) FROM personal.silver.events_cleaned
  UNION ALL SELECT 'certifications', count(*) FROM personal.silver.certifications_cleaned
  UNION ALL SELECT 'education', count(*) FROM personal.silver.education_cleaned
  UNION ALL SELECT 'member_follows', count(*) FROM personal.silver.member_follows_cleaned
),
agg AS (
  SELECT
    sum(CASE WHEN metric = 'positions' THEN value END) AS positions,
    sum(CASE WHEN metric = 'skills' THEN value END) AS skills,
    sum(CASE WHEN metric = 'languages' THEN value END) AS languages,
    sum(CASE WHEN metric = 'projects' THEN value END) AS projects,
    sum(CASE WHEN metric = 'publications' THEN value END) AS publications,
    sum(CASE WHEN metric = 'events' THEN value END) AS events,
    sum(CASE WHEN metric = 'certifications' THEN value END) AS certifications,
    sum(CASE WHEN metric = 'education' THEN value END) AS education,
    sum(CASE WHEN metric = 'member_follows' THEN value END) AS members_followed
  FROM cnt
)
SELECT
  p.*,
  r.member_since,
  agg.*
FROM p, r, agg