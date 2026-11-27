-- BRONZE: followed LinkedIn organizations
CREATE OR REFRESH STREAMING TABLE personal.bronze.company_follows_raw
COMMENT 'Raw LinkedIn company follows export, one row per followed organization plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Organization` AS organization,
  `Followed On` AS followed_on_string,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Company Follows.csv'
)