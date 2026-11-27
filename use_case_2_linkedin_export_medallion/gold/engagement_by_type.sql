-- GOLD: feed engagement by event type (and subtype, e.g. reaction kind)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.engagement_by_type
COMMENT 'Engagement events per event type and subtype, with share of all events'
AS
WITH per_type AS (
  SELECT
    event_type,
    subtype,
    count(*) AS events
  FROM personal.silver.engagement_cleaned
  GROUP BY ALL
)
SELECT
  event_type,
  subtype,
  events,
  round(events * 100.0 / sum(events) OVER (), 2) AS share_pct
FROM per_type