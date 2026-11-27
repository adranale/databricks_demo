-- BRONZE: LinkedIn security challenges (identity verification events)
CREATE OR REFRESH STREAMING TABLE personal.bronze.security_challenges_raw
COMMENT 'Raw security challenges export, one row per challenge event (all-string columns) plus file metadata'
-- The IP address is kept for raw-layer completeness but dropped in silver (privacy).
AS SELECT
  `Challenge Date` AS challenge_date_string,
  `IP Address` AS ip_address,
  `User Agent` AS user_agent,
  `Country` AS country,
  `Challenge Type` AS challenge_type,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Security Challenges.csv'
)