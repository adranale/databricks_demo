-- BRONZE: LinkedIn invitations (pending sent/received invites)
CREATE OR REFRESH STREAMING TABLE personal.bronze.invitations_raw
COMMENT 'Raw LinkedIn invitations export, one row per invitation (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `From` AS from_name,
  `To` AS to_name,
  `Sent At` AS sent_at_string,
  `Message` AS message,
  `Direction` AS direction,
  `inviterProfileUrl` AS inviter_profile_url,
  `inviteeProfileUrl` AS invitee_profile_url,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Invitations.csv'
)