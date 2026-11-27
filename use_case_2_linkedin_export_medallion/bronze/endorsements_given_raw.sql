-- BRONZE: LinkedIn skill endorsements given to others
CREATE OR REFRESH STREAMING TABLE personal.bronze.endorsements_given_raw
COMMENT 'Raw endorsements given, one row per endorsement (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Endorsement Date` AS endorsement_date_string,
  `Skill Name` AS skill_name,
  `Endorsee First Name` AS endorsee_first_name,
  `Endorsee Last Name` AS endorsee_last_name,
  `Endorsee Public Url` AS endorsee_public_url,
  `Endorsement Status` AS endorsement_status,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Endorsement_Given_Info.csv'
)