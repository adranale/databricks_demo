-- GOLD: invitations sent vs received
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.invitations_summary
COMMENT 'Pending invitations by direction, with share and share carrying a personal message'
AS
SELECT
  direction,
  count(*) AS invitations,
  round(count(*) * 100.0 / sum(count(*)) OVER (), 2) AS share_pct,
  round(avg(CASE WHEN has_message THEN 100.0 ELSE 0.0 END), 1) AS with_message_pct
FROM personal.silver.invitations_cleaned
GROUP BY direction