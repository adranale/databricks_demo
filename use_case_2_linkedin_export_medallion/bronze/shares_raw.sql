-- BRONZE: LinkedIn shares (own posts and reshares to the feed)
CREATE OR REFRESH STREAMING TABLE personal.bronze.shares_raw
COMMENT 'Raw LinkedIn shares export, one row per share (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `Date` AS shared_at_string,
  `ShareLink` AS share_link,
  `ShareCommentary` AS share_commentary,
  `SharedUrl` AS shared_url,
  `MediaUrl` AS media_url,
  `Visibility` AS visibility,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Shares.csv'
)