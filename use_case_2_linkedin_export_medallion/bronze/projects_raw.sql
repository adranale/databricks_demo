-- BRONZE: LinkedIn profile projects
CREATE OR REFRESH STREAMING TABLE personal.bronze.projects_raw
COMMENT 'Raw projects export, one row per profile project (all-string columns) plus file metadata'
AS SELECT
  `Title` AS title,
  `Description` AS description,
  `Url` AS url,
  `Started On` AS started_on_string,
  `Finished On` AS finished_on_string,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Projects.csv'
)