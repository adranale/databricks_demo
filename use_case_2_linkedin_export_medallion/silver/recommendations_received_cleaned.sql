-- SILVER: cleaned recommendations received (author, company, title, text, parsed date)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.recommendations_received_cleaned
COMMENT 'One row per recommendation: author, company, job title, text, parsed creation date'
AS
WITH typed AS (
  SELECT
    concat_ws(' ', trim(author_first_name), trim(author_last_name)) AS author,
    nullif(trim(company), '') AS company,
    nullif(trim(job_title), '') AS job_title,
    text,
    -- 'M/d' (not 'MM/dd') so single-digit days/months parse
    try_to_timestamp(creation_date_string, 'M/d/yy, h:mm a') AS created_at,
    upper(trim(status)) AS status
  FROM personal.bronze.recommendations_received_raw
)
SELECT DISTINCT
  author,
  company,
  job_title,
  text,
  created_at,
  date(created_at) AS created_date,
  status = 'VISIBLE' AS is_visible
FROM typed
WHERE created_at IS NOT NULL