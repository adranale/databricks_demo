-- GOLD: career history timeline (row-level)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.career_timeline
COMMENT 'One row per career position with company, title, location, start/end dates and current flag'
AS
SELECT
  company_name,
  title,
  location,
  started_on,
  finished_on,
  is_current
FROM personal.silver.positions_cleaned