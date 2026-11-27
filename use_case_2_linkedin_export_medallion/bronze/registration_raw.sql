-- BRONZE: LinkedIn registration (account creation)
CREATE OR REFRESH STREAMING TABLE personal.bronze.registration_raw
COMMENT 'Raw registration export, one row with the account creation timestamp (all-string columns) plus file metadata'
-- The registration IP is deliberately NOT ingested (PII, no analytic value).
AS SELECT
  `Registered At` AS registered_at_string,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Registration.csv'
)