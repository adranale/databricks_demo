-- BRONZE: LinkedIn profile (headline, industry, location, summary)
CREATE OR REFRESH STREAMING TABLE personal.bronze.profile_raw
COMMENT 'Raw profile export, one row with selected profile fields (all-string columns) plus file metadata'
-- Deliberately ingests only non-PII fields: name, address, birth date are NOT ingested.
AS SELECT
  `Headline` AS headline,
  `Industry` AS industry,
  `Geo Location` AS geo_location,
  `Summary` AS summary,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Profile.csv'
)