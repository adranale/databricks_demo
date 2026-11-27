-- GOLD LAYER: violation types (Tatbestände)
-- Labels are best-effort for the two dominant documented code families; all other codes stay unlabeled.
-- 11314x: parking on the sidewalk; 1413xx: parking time limit exceeded (graduated by duration)
CREATE OR REFRESH MATERIALIZED VIEW opendata.gold.strafzettel_by_violation
COMMENT 'Tickets by primary violation code with best-effort labels for the dominant code families'
AS
WITH per_code AS (
  SELECT
    violation_code_1 AS violation_code,
    count(*) AS tickets
  FROM opendata.silver.strafzettel_cleaned
  GROUP BY violation_code_1
)
SELECT
  violation_code,
  CASE
    WHEN violation_code LIKE '11314%' THEN 'Parken auf dem Gehweg / parking on the sidewalk'
    WHEN violation_code LIKE '1413%' THEN 'Parkdauer überschritten / parking time limit exceeded'
    ELSE NULL
  END AS violation_label,
  tickets,
  round(tickets * 100.0 / sum(tickets) OVER (), 2) AS share_pct
FROM per_code