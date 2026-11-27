-- SILVER: cleaned message metadata (content kept, timestamps parsed, own-vs-other flag)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.messages_cleaned
COMMENT 'One row per message: conversation, sender, parsed timestamp, content length, own-message flag'
AS
WITH account_holder AS (
  -- The account holder is derived from the data, never hardcoded: every received
  -- message in a personal LinkedIn export is addressed TO the account holder, so
  -- their name is by far the most frequent value in the TO column.
  SELECT trim(recipients) AS account_holder_name
  FROM personal.bronze.messages_raw
  WHERE recipients IS NOT NULL AND trim(recipients) <> ''
  GROUP BY trim(recipients)
  ORDER BY count(*) DESC
  LIMIT 1
),
typed AS (
  SELECT
    trim(conversation_id) AS conversation_id,
    trim(conversation_title) AS conversation_title,
    trim(sender) AS sender,
    trim(sender_profile_url) AS sender_profile_url,
    trim(recipients) AS recipients,
    try_to_timestamp(substring(sent_at_string, 1, 19), 'yyyy-MM-dd HH:mm:ss') AS sent_at,
    nullif(trim(subject), '') AS subject,
    content AS content,
    trim(sender) = account_holder_name AS is_own_message
  FROM personal.bronze.messages_raw, account_holder
)
SELECT DISTINCT
  conversation_id,
  conversation_title,
  sender,
  sender_profile_url,
  recipients,
  sent_at,
  date(sent_at) AS sent_date,
  year(sent_at) AS sent_year,
  month(sent_at) AS sent_month,
  subject,
  content,
  length(content) AS content_chars,
  is_own_message
FROM typed
WHERE sent_at IS NOT NULL
