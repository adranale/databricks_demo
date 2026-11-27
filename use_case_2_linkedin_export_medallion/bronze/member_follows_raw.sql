-- BRONZE: LinkedIn followed members (people, not companies)
CREATE OR REFRESH STREAMING TABLE personal.bronze.member_follows_raw
COMMENT 'Raw member follows export, one row per followed member (all-string columns) plus file metadata'
AS SELECT
  `Date` AS followed_at_string,
  `Status` AS status,
  `FullName` AS full_name,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Member_Follows.csv'
)