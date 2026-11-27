-- BRONZE: LinkedIn comments written on feed content
CREATE OR REFRESH STREAMING TABLE personal.bronze.comments_raw
COMMENT 'Raw LinkedIn comments export, one row per comment (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Date` AS comment_at_string,
  `Link` AS link,
  `Message` AS message,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Comments.csv'
)