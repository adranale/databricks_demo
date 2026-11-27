-- SILVER: skill endorsements received (parsed timestamps, endorser name, status)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.endorsements_received_cleaned
COMMENT 'One row per endorsement received: skill, endorser, parsed timestamp, status'
AS
WITH typed AS (
  SELECT
    trim(skill_name) AS skill_name,
    concat_ws(' ', trim(endorser_first_name), trim(endorser_last_name)) AS endorser_name,
    -- 'yyyy/MM/dd HH:mm:ss UTC' -> parse the local part only
    try_to_timestamp(substring(endorsement_date_string, 1, 19), 'yyyy/MM/dd HH:mm:ss') AS endorsement_at,
    upper(trim(endorsement_status)) AS endorsement_status
  FROM personal.bronze.endorsements_received_raw
)
SELECT DISTINCT
  skill_name,
  endorser_name,
  endorsement_at,
  date(endorsement_at) AS endorsement_date,
  endorsement_status
FROM typed
WHERE endorsement_at IS NOT NULL AND skill_name <> ''