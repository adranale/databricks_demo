-- BRONZE: education history (schools and degrees)
CREATE OR REFRESH STREAMING TABLE personal.bronze.education_raw
COMMENT 'Raw education export, one row per school (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `School Name` AS school_name,
  `Start Date` AS start_date_string,
  `End Date` AS end_date_string,
  `Notes` AS notes,
  `Degree Name` AS degree_name,
  `Activities` AS activities,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Education.csv'
)