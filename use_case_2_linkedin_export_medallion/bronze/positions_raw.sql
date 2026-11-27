-- BRONZE: LinkedIn positions (work history)
CREATE OR REFRESH STREAMING TABLE personal.bronze.positions_raw
COMMENT 'Raw LinkedIn positions export, one row per position (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Company Name` AS company_name,
  `Title` AS title,
  `Description` AS description,
  `Location` AS location,
  `Started On` AS started_on_string,
  `Finished On` AS finished_on_string,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Positions.csv'
)