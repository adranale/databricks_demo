-- SILVER: skill endorsements given to others (parsed timestamps, endorsee name, status)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.endorsements_given_cleaned
COMMENT 'One row per endorsement given: skill, endorsee, parsed timestamp, status'
AS
WITH typed AS (
  SELECT
    trim(skill_name) AS skill_name,
    concat_ws(' ', trim(endorsee_first_name), trim(endorsee_last_name)) AS endorsee_name,
    -- 'yyyy/MM/dd HH:mm:ss UTC' -> parse the local part only
    try_to_timestamp(substring(endorsement_date_string, 1, 19), 'yyyy/MM/dd HH:mm:ss') AS endorsement_at,
    upper(trim(endorsement_status)) AS endorsement_status
  FROM personal.bronze.endorsements_given_raw
)
SELECT DISTINCT
  skill_name,
  endorsee_name,
  endorsement_at,
  date(endorsement_at) AS endorsement_date,
  endorsement_status
FROM typed
WHERE endorsement_at IS NOT NULL AND skill_name <> ''