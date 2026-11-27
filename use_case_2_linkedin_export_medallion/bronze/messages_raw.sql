-- BRONZE: LinkedIn messages (all conversations, full history)
CREATE OR REFRESH STREAMING TABLE personal.bronze.messages_raw
COMMENT 'Raw LinkedIn messages export, one row per message (all-string columns) plus file metadata'
-- CSV headers with spaces are renamed to snake_case (Delta rejects spaces in column names).
AS SELECT
  `CONVERSATION ID` AS conversation_id,
  `CONVERSATION TITLE` AS conversation_title,
  `FROM` AS sender,
  `SENDER PROFILE URL` AS sender_profile_url,
  `TO` AS recipients,
  `RECIPIENT PROFILE URLS` AS recipient_profile_urls,
  `DATE` AS sent_at_string,
  `SUBJECT` AS subject,
  `CONTENT` AS content,
  `FOLDER` AS folder,
  `ATTACHMENTS` AS attachments,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'messages.csv'
)