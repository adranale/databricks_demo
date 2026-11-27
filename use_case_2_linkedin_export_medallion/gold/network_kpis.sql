-- GOLD: headline network KPIs (single row)
CREATE OR REFRESH MATERIALIZED VIEW personal.gold.network_kpis
COMMENT 'Headline KPIs: connections, companies, titles, messages, conversations, invitations, top company'
AS
WITH base AS (
  SELECT
    count(*) AS total_connections,
    count(DISTINCT company) AS distinct_companies,
    count(DISTINCT position) AS distinct_positions,
    min(connected_date) AS first_connection,
    max(connected_date) AS latest_connection,
    sum(CASE WHEN connected_year >= 2024 THEN 1 ELSE 0 END) AS connections_since_2024
  FROM personal.silver.connections_cleaned
),
msg AS (
  SELECT
    count(*) AS total_messages,
    count(DISTINCT conversation_id) AS conversations
  FROM personal.silver.messages_cleaned
),
inv AS (
  SELECT
    sum(CASE WHEN direction = 'OUTGOING' THEN 1 ELSE 0 END) AS outgoing_invitations,
    sum(CASE WHEN direction = 'INCOMING' THEN 1 ELSE 0 END) AS incoming_invitations
  FROM personal.silver.invitations_cleaned
),
top_company AS (
  SELECT max(struct(connections, company)) AS c
  FROM (
    SELECT company, count(*) AS connections
    FROM personal.silver.connections_cleaned
    WHERE company IS NOT NULL
    GROUP BY company
  )
)
SELECT
  base.*,
  msg.total_messages,
  msg.conversations,
  inv.outgoing_invitations,
  inv.incoming_invitations,
  top_company.c.company AS top_company,
  top_company.c.connections AS top_company_connections
FROM base, msg, inv, top_company