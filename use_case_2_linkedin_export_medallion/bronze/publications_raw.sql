-- BRONZE: LinkedIn publications
CREATE OR REFRESH STREAMING TABLE personal.bronze.publications_raw
COMMENT 'Raw publications export, one row per publication (all-string columns) plus file metadata'
AS SELECT
  `Name` AS publication_name,
  `Published On` AS published_on_string,
  `Description` AS description,
  `Publisher` AS publisher,
  `Url` AS url,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Publications.csv'
)