-- SILVER: unified feed engagement events (reactions, comments, shares, reposts, poll votes)
CREATE OR REFRESH MATERIALIZED VIEW personal.silver.engagement_cleaned
COMMENT 'One row per engagement event with event type, subtype and parsed timestamp'
AS
SELECT DISTINCT
  'REACTION' AS event_type,
  upper(trim(reaction_type)) AS subtype,
  try_to_timestamp(reaction_at_string, 'yyyy-MM-dd HH:mm:ss') AS occurred_at,
  link
FROM personal.bronze.reactions_raw
WHERE reaction_at_string IS NOT NULL AND trim(reaction_at_string) <> ''
UNION ALL
SELECT DISTINCT
  'COMMENT' AS event_type,
  NULL AS subtype,
  try_to_timestamp(comment_at_string, 'yyyy-MM-dd HH:mm:ss') AS occurred_at,
  link
FROM personal.bronze.comments_raw
WHERE comment_at_string IS NOT NULL AND trim(comment_at_string) <> ''
UNION ALL
SELECT DISTINCT
  'SHARE' AS event_type,
  upper(trim(visibility)) AS subtype,
  try_to_timestamp(shared_at_string, 'yyyy-MM-dd HH:mm:ss') AS occurred_at,
  share_link
FROM personal.bronze.shares_raw
WHERE shared_at_string IS NOT NULL AND trim(shared_at_string) <> ''
UNION ALL
SELECT DISTINCT
  'REPOST' AS event_type,
  NULL AS subtype,
  try_to_timestamp(reposted_at_string, 'yyyy-MM-dd HH:mm:ss') AS occurred_at,
  link
FROM personal.bronze.reposts_raw
WHERE reposted_at_string IS NOT NULL AND trim(reposted_at_string) <> ''
UNION ALL
SELECT DISTINCT
  'VOTE' AS event_type,
  option_text AS subtype,
  try_to_timestamp(voted_at_string, 'yyyy-MM-dd HH:mm:ss') AS occurred_at,
  link
FROM personal.bronze.votes_raw
WHERE voted_at_string IS NOT NULL AND trim(voted_at_string) <> ''