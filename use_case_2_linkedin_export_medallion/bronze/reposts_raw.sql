-- BRONZE: LinkedIn instant reposts of feed content
CREATE OR REFRESH STREAMING TABLE personal.bronze.reposts_raw
COMMENT 'Raw LinkedIn instant reposts export, one row per repost (all-string columns) plus file metadata'
AS SELECT
  `Date` AS reposted_at_string,
  `Link` AS link,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'InstantReposts.csv'
)