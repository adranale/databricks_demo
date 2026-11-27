-- BRONZE: recommendations received from other members
CREATE OR REFRESH STREAMING TABLE personal.bronze.recommendations_received_raw
COMMENT 'Raw recommendations received, one row per recommendation (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `First Name` AS author_first_name,
  `Last Name` AS author_last_name,
  `Company` AS company,
  `Job Title` AS job_title,
  `Text` AS text,
  `Creation Date` AS creation_date_string,
  `Status` AS status,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Recommendations_Received.csv'
)