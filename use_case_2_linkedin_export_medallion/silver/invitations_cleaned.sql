-- SILVER: cleaned invitations (direction, parsed sent timestamp, message flag, contact)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.invitations_cleaned
COMMENT 'One row per invitation: direction, inviter/invitee, sent timestamp, whether it carried a message'
AS
WITH typed AS (
  SELECT
    -- 'MM/d' (not 'MM/dd') so single-digit days like '11/6/26' parse
    try_to_timestamp(sent_at_string, 'MM/d/yy, h:mm a') AS sent_at,
    upper(trim(direction)) AS direction,
    trim(from_name) AS inviter,
    trim(to_name) AS invitee,
    nullif(trim(message), '') AS message,
    trim(inviter_profile_url) AS inviter_profile_url,
    trim(invitee_profile_url) AS invitee_profile_url,
    CASE WHEN upper(trim(direction)) = 'OUTGOING' THEN trim(to_name) ELSE trim(from_name) END AS contact
  FROM personal.bronze.invitations_raw
)
SELECT DISTINCT
  sent_at,
  date(sent_at) AS sent_date,
  direction,
  inviter,
  invitee,
  contact,
  message IS NOT NULL AS has_message,
  message,
  inviter_profile_url,
  invitee_profile_url
FROM typed
WHERE sent_at IS NOT NULL