-- BRONZE: LinkedIn Learning content history (courses, videos)
-- NOTE: the source CSV has a trailing empty header column; it is not selected here.
CREATE OR REFRESH STREAMING TABLE personal.bronze.learning_raw
COMMENT 'Raw LinkedIn Learning history, one row per content item (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Content Title` AS content_title,
  `Content Description` AS content_description,
  `Content Type` AS content_type,
  `Content Last Watched Date (if viewed)` AS last_watched_at_string,
  `Content Completed At (if completed)` AS completed_at_string,
  `Content Saved` AS content_saved,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Learning.csv'
)