-- BRONZE: LinkedIn skills
CREATE OR REFRESH STREAMING TABLE personal.bronze.skills_raw
COMMENT 'Raw LinkedIn skills export, one row per listed skill (all-string columns) plus file metadata'
-- CSV header renamed to snake_case.
AS SELECT
  `Name` AS skill_name,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  pathGlobFilter => 'Skills.csv'
)