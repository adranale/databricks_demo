-- BRONZE: LinkedIn skill endorsements received from others
CREATE OR REFRESH STREAMING TABLE personal.bronze.endorsements_received_raw
COMMENT 'Raw endorsements received, one row per endorsement (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Endorsement Date` AS endorsement_date_string,
  `Skill Name` AS skill_name,
  `Endorser First Name` AS endorser_first_name,
  `Endorser Last Name` AS endorser_last_name,
  `Endorser Public Url` AS endorser_public_url,
  `Endorsement Status` AS endorsement_status,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Endorsement_Received_Info.csv'
)