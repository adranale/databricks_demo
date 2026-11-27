-- BRONZE: LinkedIn poll votes
CREATE OR REFRESH STREAMING TABLE personal.bronze.votes_raw
COMMENT 'Raw LinkedIn poll votes export, one row per vote (all-string columns) plus file metadata'
AS SELECT
  `Date` AS voted_at_string,
  `Link` AS link,
  `OptionText` AS option_text,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Votes.csv'
)