-- BRONZE: LinkedIn profile languages
CREATE OR REFRESH STREAMING TABLE personal.bronze.languages_raw
COMMENT 'Raw languages export, one row per spoken language (all-string columns) plus file metadata'
AS SELECT
  `Name` AS language_name,
  `Proficiency` AS proficiency,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Languages.csv'
)