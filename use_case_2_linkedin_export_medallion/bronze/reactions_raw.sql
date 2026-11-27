-- BRONZE: LinkedIn reactions (likes etc. on feed content)
CREATE OR REFRESH STREAMING TABLE personal.bronze.reactions_raw
COMMENT 'Raw LinkedIn reactions export, one row per reaction (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Date` AS reaction_at_string,
  `Type` AS reaction_type,
  `Link` AS link,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Reactions.csv'
)