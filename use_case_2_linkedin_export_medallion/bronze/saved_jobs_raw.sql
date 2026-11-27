-- BRONZE: LinkedIn saved jobs (job-search activity)
CREATE OR REFRESH STREAMING TABLE personal.bronze.saved_jobs_raw
COMMENT 'Raw saved jobs export, one row per saved job posting (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Saved Date` AS saved_at_string,
  `Job Url` AS job_url,
  `Job Title` AS job_title,
  `Company Name` AS company_name,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Saved Jobs.csv'
)