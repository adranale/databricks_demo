-- BRONZE: certifications (obtained credentials)
CREATE OR REFRESH STREAMING TABLE personal.bronze.certifications_raw
COMMENT 'Raw certifications export, one row per certification (all-string columns) plus file metadata'
AS SELECT
  `Name` AS certification_name,
  `Url` AS url,
  `Authority` AS authority,
  `Started On` AS started_on_string,
  `Finished On` AS finished_on_string,
  `License Number` AS license_number,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Certifications.csv'
)