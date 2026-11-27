-- GOLD: saved jobs per company (job-search interest by employer)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.saved_jobs_by_company
COMMENT 'Saved job postings per company with latest save date and share'
AS
WITH per_company AS (
  SELECT
    company_name,
    count(*) AS jobs_saved,
    max(saved_date) AS latest_saved
  FROM personal.silver.saved_jobs_cleaned
  WHERE company_name IS NOT NULL
  GROUP BY company_name
)
SELECT
  company_name,
  jobs_saved,
  latest_saved,
  round(jobs_saved * 100.0 / sum(jobs_saved) OVER (), 2) AS share_pct
FROM per_company