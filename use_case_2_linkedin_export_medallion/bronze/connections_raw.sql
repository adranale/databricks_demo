-- BRONZE: LinkedIn Connections export (has a 3-line preamble before the header)
CREATE OR REFRESH STREAMING TABLE personal.bronze.connections_raw
COMMENT 'Raw LinkedIn connections export, one row per 1st-degree connection (all-string columns) plus file metadata'
-- CSV headers contain spaces ('First Name', ...) which Delta column names reject,
-- so every column is renamed to snake_case at ingestion.
AS SELECT
  `First Name` AS first_name,
  `Last Name` AS last_name,
  `URL` AS profile_url,
  `Email Address` AS email_address,
  `Company` AS company,
  `Position` AS position,
  `Connected On` AS connected_on,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  skipRows => 3,
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Connections.csv'
)