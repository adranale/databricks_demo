-- SILVER: cleaned saved jobs (parsed save timestamp, title, company)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.saved_jobs_cleaned
COMMENT 'One row per saved job: parsed save timestamp, job title, company'
AS
WITH typed AS (
  SELECT
    -- 'M/d' (not 'MM/dd') so single-digit days/months like '1/15/18' parse
    try_to_timestamp(saved_at_string, 'M/d/yy, h:mm a') AS saved_at,
    trim(job_title) AS job_title,
    nullif(trim(company_name), '') AS company_name,
    job_url
  FROM personal.bronze.saved_jobs_raw
)
SELECT DISTINCT
  saved_at,
  date(saved_at) AS saved_date,
  job_title,
  company_name,
  job_url
FROM typed
WHERE saved_at IS NOT NULL