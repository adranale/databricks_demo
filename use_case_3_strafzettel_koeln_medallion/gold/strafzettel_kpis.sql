-- GOLD LAYER: headline KPIs (single row)
CREATE OR REFRESH MATERIALIZED VIEW opendata.gold.strafzettel_kpis
COMMENT 'Headline KPIs for the 2015 ticket dataset: totals, distinct dimensions, local/weekend split, top street and violation'
AS
WITH base AS (
  SELECT
    count(*) AS total_tickets,
    count(DISTINCT street) AS distinct_streets,
    count(DISTINCT vehicle_brand) AS distinct_brands,
    count(DISTINCT violation_code_1) AS distinct_violations,
    min(ticket_date) AS first_ticket_date,
    max(ticket_date) AS last_ticket_date,
    sum(CASE WHEN plate_origin = 'Köln (K)' THEN 1 ELSE 0 END) AS local_tickets,
    sum(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_tickets
  FROM opendata.silver.strafzettel_cleaned
),
top_street AS (
  SELECT max(struct(tickets, street)) AS s
  FROM (SELECT street, count(*) AS tickets FROM opendata.silver.strafzettel_cleaned GROUP BY street)
),
top_violation AS (
  SELECT max(struct(tickets, violation_code_1)) AS v
  FROM (SELECT violation_code_1, count(*) AS tickets FROM opendata.silver.strafzettel_cleaned GROUP BY violation_code_1)
)
SELECT
  base.*,
  top_street.s.street AS top_street_name,
  top_street.s.tickets AS top_street_tickets,
  top_violation.v.violation_code_1 AS top_violation_code,
  top_violation.v.tickets AS top_violation_tickets
FROM base, top_street, top_violation