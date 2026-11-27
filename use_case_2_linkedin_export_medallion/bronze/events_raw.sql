-- BRONZE: LinkedIn events
CREATE OR REFRESH STREAMING TABLE personal.bronze.events_raw
COMMENT 'Raw events export, one row per event registration (all-string columns) plus file metadata'
AS SELECT
  `Event Name` AS event_name,
  `Event Time` AS event_time_string,
  `Status` AS status,
  `External Url` AS external_url,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Events.csv'
)