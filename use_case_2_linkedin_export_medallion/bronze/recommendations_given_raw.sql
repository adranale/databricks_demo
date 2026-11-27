-- BRONZE: LinkedIn recommendations given to others
CREATE OR REFRESH STREAMING TABLE personal.bronze.recommendations_given_raw
COMMENT 'Raw recommendations given export, one row per written recommendation (all-string columns) plus file metadata'
AS SELECT
  `First Name` AS recipient_first_name,
  `Last Name` AS recipient_last_name,
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
  pathGlobFilter => 'Recommendations_Given.csv'
)