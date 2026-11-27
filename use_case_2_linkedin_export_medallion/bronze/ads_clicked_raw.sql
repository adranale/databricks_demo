-- BRONZE: LinkedIn ads clicked (advertising engagement)
CREATE OR REFRESH STREAMING TABLE personal.bronze.ads_clicked_raw
COMMENT 'Raw ads clicked export, one row per ad click (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Ad clicked Date` AS clicked_at_string,
  `Ad Title/Id` AS ad_id,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Ads Clicked.csv'
)