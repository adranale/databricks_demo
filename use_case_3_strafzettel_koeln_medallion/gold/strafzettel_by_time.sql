-- GOLD LAYER: temporal demand pattern (day x hour grain)
CREATE OR REFRESH MATERIALIZED VIEW opendata.gold.strafzettel_by_time
COMMENT 'Tickets by day and hour with month, weekday and weekend dimensions'
AS
SELECT
  ticket_date,
  ticket_month,
  ticket_month_name,
  ticket_hour,
  weekday_num,
  weekday_name,
  is_weekend,
  count(*) AS tickets
FROM opendata.silver.strafzettel_cleaned
GROUP BY ALL