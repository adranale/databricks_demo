-- Databricks notebook source
-- DBTITLE 1,Medallion DDL: Bronze -> Silver -> Gold
-- Step 2A: Lakeflow Declarative Pipeline (Delta Live Tables): Bronze -> Silver -> Gold
-- Catalog: ops | Target Schemas: silver & gold
--
-- This declarative pipeline incrementally transforms ops.bronze -> ops.silver -> ops.gold
-- with Data Quality Expectations. Run 04_deploy_and_schedule_pipeline to automatically
-- register and schedule this pipeline every 2 hours.

-- ============================================================================
-- LAYER 1 -> 2: BRONZE TO SILVER (STREAMING TABLES + QUALITY EXPECTATIONS)
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE ops.silver.customers_cleaned (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_account_id  EXPECT (account_id IS NOT NULL)  ON VIOLATION DROP ROW
)
COMMENT 'Silver: Cleansed and standardized customer records streamed from ops.bronze.customers_raw'
AS
WITH latest AS (
  -- Streaming-safe deduplication: ROW_NUMBER() windows are not supported over
  -- streaming sources, so the most recently ingested record per customer_id is
  -- selected with MAX(struct(...)) + GROUP BY (same semantics as the original
  -- ROW_NUMBER ... ORDER BY _ingest_timestamp DESC = 1).
  SELECT MAX(STRUCT(_ingest_timestamp, raw_json)) AS rec
  FROM STREAM(ops.bronze.customers_raw)
  GROUP BY get_json_object(raw_json, '$.customer_id')
)
SELECT
  get_json_object(rec.raw_json, '$.customer_id') AS customer_id,
  INITCAP(TRIM(get_json_object(rec.raw_json, '$.full_name'))) AS customer_name,
  COALESCE(LOWER(TRIM(get_json_object(rec.raw_json, '$.email'))), 'missing_email@unknown.local') AS email,
  INITCAP(TRIM(get_json_object(rec.raw_json, '$.city'))) AS city,
  TRIM(get_json_object(rec.raw_json, '$.country')) AS country,
  TRIM(get_json_object(rec.raw_json, '$.segment')) AS segment,
  TRIM(get_json_object(rec.raw_json, '$.account_id')) AS account_id,
  CAST(get_json_object(rec.raw_json, '$.signup_date') AS DATE) AS signup_date,
  rec._ingest_timestamp AS last_updated_ts
FROM latest;

CREATE OR REFRESH STREAMING TABLE ops.silver.transactions_cleaned (
  CONSTRAINT valid_tx_id        EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT completed_tx_only  EXPECT (status = 'COMPLETED')       ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount    EXPECT (amount_usd > 0)             ON VIOLATION DROP ROW
)
COMMENT 'Silver: Deduplicated valid COMPLETED transactions streamed from ops.bronze.transactions_raw'
AS
WITH first_seen AS (
  -- Streaming-safe deduplication: keep the earliest ingested record per
  -- transaction_id (same semantics as the original ROW_NUMBER ... ORDER BY
  -- _ingest_timestamp ASC = 1), using MIN(struct(...)) + GROUP BY.
  SELECT MIN(STRUCT(_ingest_timestamp, raw_payload)) AS rec
  FROM STREAM(ops.bronze.transactions_raw)
  WHERE raw_payload IS NOT NULL
  GROUP BY get_json_object(raw_payload, '$.tx_id')
)
SELECT
  get_json_object(rec.raw_payload, '$.tx_id') AS transaction_id,
  get_json_object(rec.raw_payload, '$.c_id') AS customer_id,
  get_json_object(rec.raw_payload, '$.prod_id') AS product_id,
  TRIM(get_json_object(rec.raw_payload, '$.prod_name')) AS product_name,
  TRIM(get_json_object(rec.raw_payload, '$.category')) AS product_category,
  CAST(get_json_object(rec.raw_payload, '$.qty') AS INT) AS quantity,
  CAST(get_json_object(rec.raw_payload, '$.unit_price') AS DECIMAL(10,2)) AS unit_price_usd,
  CAST(get_json_object(rec.raw_payload, '$.amt') AS DECIMAL(12,2)) AS amount_usd,
  UPPER(TRIM(get_json_object(rec.raw_payload, '$.curr'))) AS currency,
  TRIM(get_json_object(rec.raw_payload, '$.channel')) AS sales_channel,
  TRIM(get_json_object(rec.raw_payload, '$.store')) AS store_location,
  CAST(get_json_object(rec.raw_payload, '$.ts') AS TIMESTAMP) AS transaction_ts,
  DATE(CAST(get_json_object(rec.raw_payload, '$.ts') AS TIMESTAMP)) AS transaction_date,
  UPPER(TRIM(get_json_object(rec.raw_payload, '$.st'))) AS status
FROM first_seen;

CREATE OR REFRESH MATERIALIZED VIEW ops.silver.customer_transactions
COMMENT 'Silver: Enriched customer transaction history joining customers_cleaned and transactions_cleaned'
AS
SELECT
  t.transaction_id,
  t.transaction_ts,
  t.transaction_date,
  c.customer_id,
  c.customer_name,
  c.email AS customer_email,
  c.city,
  c.country,
  c.segment AS customer_segment,
  c.account_id,
  t.product_id,
  t.product_name,
  t.product_category,
  t.quantity,
  t.unit_price_usd,
  t.amount_usd,
  t.sales_channel,
  t.store_location
FROM ops.silver.transactions_cleaned t
INNER JOIN ops.silver.customers_cleaned c
  ON t.customer_id = c.customer_id;

CREATE OR REFRESH STREAMING TABLE ops.silver.accounts_cleaned (
  CONSTRAINT valid_account_id EXPECT (account_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver: Standardized Salesforce accounts'
AS
WITH latest AS (
  -- Streaming-safe deduplication: latest ingested record per account_id
  -- via MAX(struct(...)) + GROUP BY.
  SELECT MAX(STRUCT(_ingest_timestamp, sf_account_id, account_name, industry,
                    annual_revenue_raw, account_tier, region, owner_name)) AS rec
  FROM STREAM(ops.bronze.accounts_raw)
  GROUP BY TRIM(sf_account_id)
)
SELECT
  TRIM(rec.sf_account_id) AS account_id,
  TRIM(rec.account_name) AS account_name,
  TRIM(rec.industry) AS industry,
  CAST(rec.annual_revenue_raw AS DECIMAL(15,2)) AS annual_revenue_usd,
  TRIM(rec.account_tier) AS account_tier,
  UPPER(TRIM(rec.region)) AS region,
  TRIM(rec.owner_name) AS account_owner
FROM latest;

CREATE OR REFRESH STREAMING TABLE ops.silver.opportunities_cleaned (
  CONSTRAINT valid_opp_id EXPECT (opportunity_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver: Validated Salesforce opportunities with weighted pipeline values'
AS
WITH latest AS (
  -- Streaming-safe deduplication: latest ingested record per opportunity_id
  -- via MAX(struct(...)) + GROUP BY.
  SELECT MAX(STRUCT(_ingest_timestamp, sf_opp_id, sf_account_id, opp_name, stage_name,
                    amount_raw, probability_pct, close_date, days_in_stage)) AS rec
  FROM STREAM(ops.bronze.opportunities_raw)
  GROUP BY TRIM(sf_opp_id)
)
SELECT
  TRIM(rec.sf_opp_id) AS opportunity_id,
  TRIM(rec.sf_account_id) AS account_id,
  TRIM(rec.opp_name) AS opportunity_name,
  TRIM(rec.stage_name) AS stage_name,
  CAST(rec.amount_raw AS DECIMAL(12,2)) AS amount_usd,
  rec.probability_pct,
  ROUND(CAST(rec.amount_raw AS DECIMAL(12,2)) * (rec.probability_pct / 100.0), 2) AS weighted_amount_usd,
  CAST(rec.close_date AS DATE) AS close_date,
  rec.days_in_stage,
  CASE WHEN rec.stage_name = 'Closed Won' THEN 1 ELSE 0 END AS is_won,
  CASE WHEN rec.stage_name IN ('Closed Won', 'Closed Lost') THEN 1 ELSE 0 END AS is_closed
FROM latest;

CREATE OR REFRESH MATERIALIZED VIEW ops.silver.account_opportunities
COMMENT 'Silver: Enriched B2B account opportunities joining accounts_cleaned and opportunities_cleaned'
AS
SELECT
  o.opportunity_id,
  o.opportunity_name,
  a.account_id,
  a.account_name,
  a.industry,
  a.account_tier,
  a.region,
  a.account_owner,
  o.stage_name,
  o.amount_usd,
  o.weighted_amount_usd,
  o.probability_pct,
  o.close_date,
  o.days_in_stage,
  o.is_won,
  o.is_closed
FROM ops.silver.opportunities_cleaned o
INNER JOIN ops.silver.accounts_cleaned a
  ON o.account_id = a.account_id;

CREATE OR REFRESH STREAMING TABLE ops.silver.leads_cleaned (
  CONSTRAINT valid_lead_id        EXPECT (lead_id IS NOT NULL)            ON VIOLATION DROP ROW,
  CONSTRAINT active_leads_only    EXPECT (lead_status <> 'Disqualified') ON VIOLATION DROP ROW
)
COMMENT 'Silver: Cleaned active marketing and sales leads'
AS
WITH latest AS (
  -- Streaming-safe deduplication: latest ingested record per lead_id
  -- via MAX(struct(...)) + GROUP BY.
  SELECT MAX(STRUCT(_ingest_timestamp, sf_lead_id, lead_name, company_name, email,
                    lead_source, status, estimated_budget_raw, region, created_date)) AS rec
  FROM STREAM(ops.bronze.leads_raw)
  GROUP BY TRIM(sf_lead_id)
)
SELECT
  TRIM(rec.sf_lead_id) AS lead_id,
  INITCAP(TRIM(rec.lead_name)) AS lead_name,
  TRIM(rec.company_name) AS company_name,
  COALESCE(LOWER(TRIM(rec.email)), 'no_email@lead.local') AS email,
  TRIM(rec.lead_source) AS lead_source,
  TRIM(rec.status) AS lead_status,
  CAST(rec.estimated_budget_raw AS DECIMAL(12,2)) AS estimated_budget_usd,
  UPPER(TRIM(rec.region)) AS region,
  CAST(rec.created_date AS DATE) AS created_date
FROM latest;

-- ============================================================================
-- LAYER 2 -> 3: SILVER TO GOLD (MATERIALIZED VIEWS FOR BI & AI)
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ops.gold.customer_spending
COMMENT 'Gold: Customer 360 spending, RFM segmentation, and churn risk propensity'
AS
WITH tx_agg AS (
  SELECT
    customer_id,
    MAX(customer_name) AS customer_name,
    MAX(customer_email) AS customer_email,
    MAX(city) AS city,
    MAX(country) AS country,
    MAX(customer_segment) AS customer_segment,
    MAX(account_id) AS account_id,
    COUNT(DISTINCT transaction_id) AS total_orders,
    SUM(quantity) AS total_units_purchased,
    SUM(amount_usd) AS total_spending_usd,
    ROUND(AVG(amount_usd), 2) AS avg_order_value_usd,
    MIN(transaction_date) AS first_purchase_date,
    MAX(transaction_date) AS last_purchase_date,
    DATEDIFF(CURRENT_DATE(), MAX(transaction_date)) AS days_since_last_purchase,
    COUNT(DISTINCT product_category) AS distinct_categories_bought
  FROM ops.silver.customer_transactions
  GROUP BY customer_id
)
SELECT
  t.*,
  a.account_name,
  a.account_tier,
  a.region,
  ROUND(
    LEAST(0.95, GREATEST(0.05,
      (t.days_since_last_purchase / 120.0) * 0.65 +
      (CASE WHEN t.total_orders = 1 THEN 0.25 ELSE 0.05 END)
    )), 2
  ) AS churn_risk_score,
  CASE
    WHEN t.total_spending_usd >= 3000 AND t.days_since_last_purchase <= 30 THEN 'VIP Champion'
    WHEN t.days_since_last_purchase > 45 THEN 'At-Risk / Dormant'
    WHEN t.total_orders >= 2 THEN 'Loyal Growth'
    ELSE 'New / Emerging'
  END AS rfm_segment
FROM tx_agg t
LEFT JOIN ops.silver.accounts_cleaned a
  ON t.account_id = a.account_id;

CREATE OR REFRESH MATERIALIZED VIEW ops.gold.account_performance
COMMENT 'Gold: B2B Account Performance combining realized product spend and Salesforce opportunities'
AS
WITH acct_tx AS (
  SELECT
    account_id,
    SUM(amount_usd) AS realized_product_revenue_usd,
    COUNT(DISTINCT transaction_id) AS product_orders_count,
    COUNT(DISTINCT customer_id) AS active_buyers_count
  FROM ops.silver.customer_transactions
  GROUP BY account_id
),
acct_opps AS (
  SELECT
    account_id,
    COUNT(DISTINCT opportunity_id) AS total_opportunities,
    SUM(CASE WHEN is_won = 1 THEN amount_usd ELSE 0 END) AS crm_won_revenue_usd,
    SUM(CASE WHEN is_closed = 0 THEN amount_usd ELSE 0 END) AS open_pipeline_usd,
    SUM(CASE WHEN is_closed = 0 THEN weighted_amount_usd ELSE 0 END) AS weighted_pipeline_usd,
    MAX(days_in_stage) AS max_days_in_stage
  FROM ops.silver.account_opportunities
  GROUP BY account_id
)
SELECT
  a.account_id,
  a.account_name,
  a.industry,
  a.account_tier,
  a.region,
  a.account_owner,
  a.annual_revenue_usd AS client_annual_revenue_usd,
  COALESCE(tx.realized_product_revenue_usd, 0) AS realized_product_revenue_usd,
  COALESCE(op.crm_won_revenue_usd, 0) AS crm_won_revenue_usd,
  COALESCE(tx.realized_product_revenue_usd, 0) + COALESCE(op.crm_won_revenue_usd, 0) AS total_account_value_usd,
  COALESCE(op.open_pipeline_usd, 0) AS open_pipeline_usd,
  COALESCE(op.weighted_pipeline_usd, 0) AS weighted_pipeline_usd,
  COALESCE(tx.active_buyers_count, 0) AS active_buyers_count,
  COALESCE(op.max_days_in_stage, 0) AS max_days_in_stage
FROM ops.silver.accounts_cleaned a
LEFT JOIN acct_tx tx ON a.account_id = tx.account_id
LEFT JOIN acct_opps op ON a.account_id = op.account_id;

CREATE OR REFRESH MATERIALIZED VIEW ops.gold.sales_pipeline_summary
COMMENT 'Gold: Regional sales pipeline funnel and active marketing lead budget'
AS
WITH opp_summary AS (
  SELECT
    region,
    stage_name,
    COUNT(DISTINCT opportunity_id) AS opportunity_count,
    SUM(amount_usd) AS pipeline_amount_usd,
    SUM(weighted_amount_usd) AS weighted_pipeline_usd,
    ROUND(AVG(days_in_stage), 1) AS avg_days_in_stage
  FROM ops.silver.account_opportunities
  GROUP BY region, stage_name
),
lead_summary AS (
  SELECT
    region,
    COUNT(DISTINCT lead_id) AS active_leads_count,
    SUM(estimated_budget_usd) AS total_lead_budget_usd
  FROM ops.silver.leads_cleaned
  GROUP BY region
)
SELECT
  o.region,
  o.stage_name,
  o.opportunity_count,
  o.pipeline_amount_usd,
  o.weighted_pipeline_usd,
  o.avg_days_in_stage,
  COALESCE(l.active_leads_count, 0) AS regional_active_leads,
  COALESCE(l.total_lead_budget_usd, 0) AS regional_lead_budget_usd
FROM opp_summary o
LEFT JOIN lead_summary l ON o.region = l.region;

CREATE OR REFRESH MATERIALIZED VIEW ops.gold.business_summary
COMMENT 'Gold: Executive regional business KPI summary combining Customer Spending and Account Performance'
AS
WITH regional_spend AS (
  SELECT
    COALESCE(region, 'OTHER') AS region,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(total_orders) AS total_orders,
    SUM(total_spending_usd) AS total_product_spend_usd,
    ROUND(AVG(churn_risk_score), 2) AS avg_churn_risk_score
  FROM ops.gold.customer_spending
  GROUP BY COALESCE(region, 'OTHER')
),
regional_accounts AS (
  SELECT
    region,
    COUNT(DISTINCT account_id) AS total_accounts,
    SUM(crm_won_revenue_usd) AS total_crm_won_usd,
    SUM(open_pipeline_usd) AS total_open_pipeline_usd,
    SUM(total_account_value_usd) AS combined_business_value_usd
  FROM ops.gold.account_performance
  GROUP BY region
)
SELECT
  a.region,
  a.total_accounts,
  COALESCE(s.total_customers, 0) AS total_customers,
  COALESCE(s.total_orders, 0) AS total_orders,
  COALESCE(s.total_product_spend_usd, 0) AS total_product_spend_usd,
  a.total_crm_won_usd,
  a.total_open_pipeline_usd,
  a.combined_business_value_usd,
  COALESCE(s.avg_churn_risk_score, 0.0) AS avg_churn_risk_score,
  CURRENT_TIMESTAMP() AS report_generated_at
FROM regional_accounts a
LEFT JOIN regional_spend s ON a.region = s.region;

-- COMMAND ----------

-- DBTITLE 1,Semi-Structured (JSON Auto Loader) & Unstructured (AI Document Parsing) Sources
-- ============================================================================
-- DEMO EXTENSION 1: SEMI-STRUCTURED SOURCE (JSON PRODUCT CATALOG, AUTO LOADER)
-- Product catalog JSONL files land in UC volume
-- /Volumes/ops/bronze/ingest/product_catalog_json/ and are ingested
-- INCREMENTALLY by Auto Loader: every pipeline update processes only the
-- files that arrived since the previous one.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE ops.bronze.product_catalog_raw
COMMENT 'Bronze: Raw semi-structured product catalog JSONL files ingested incrementally from the landing volume by Auto Loader'
AS
SELECT
  *,
  _metadata.file_name AS _source_file,
  _metadata.file_modification_time AS _source_file_mtime
FROM STREAM read_files(
  '/Volumes/ops/bronze/ingest/product_catalog_json/',
  format => 'json',
  pathGlobFilter => '*.jsonl'
);

-- Silver: flatten, validate and deduplicate the catalog.
-- Multiple catalog batches re-emit the same products with new prices, so the
-- NEWEST catalog update per product_id must win. File mtimes are unreliable on
-- cloud storage, so dedup keys on the business field updated_at.
CREATE OR REFRESH STREAMING TABLE ops.silver.product_catalog_cleaned (
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_list_price  EXPECT (list_price_usd > 0)     ON VIOLATION DROP ROW,
  CONSTRAINT usd_priced        EXPECT (currency = 'USD')       ON VIOLATION DROP ROW
)
COMMENT 'Silver: Cleaned and deduplicated product catalog - latest catalog update wins per product (semi-structured source)'
AS
WITH latest AS (
  -- Streaming-safe deduplication: newest catalog update per product_id wins
  -- via MAX(struct(...)) + GROUP BY (updated_at is the leading field).
  -- Records missing optional fields (specs, promo, description) are handled
  -- with COALESCE downstream - classic semi-structured messiness.
  SELECT MAX(STRUCT(
    CAST(updated_at AS TIMESTAMP) AS updated_ts,
    product_id, product_name, category, brand, list_price_usd, currency,
    status, description, specs, promo, _source_file
  )) AS rec
  FROM STREAM(ops.bronze.product_catalog_raw)
  GROUP BY product_id
)
SELECT
  TRIM(rec.product_id) AS product_id,
  TRIM(rec.product_name) AS product_name,
  TRIM(rec.category) AS product_category,
  TRIM(rec.brand) AS brand,
  CAST(rec.list_price_usd AS DECIMAL(10,2)) AS list_price_usd,
  UPPER(TRIM(rec.currency)) AS currency,
  LOWER(TRIM(rec.status)) AS product_status,
  rec.updated_ts AS catalog_updated_at,
  TRIM(COALESCE(rec.description, 'No description provided')) AS description,
  CAST(COALESCE(rec.specs.warranty_months, 0) AS INT) AS warranty_months,
  CAST(COALESCE(rec.promo.discount_pct, 0) AS INT) AS active_discount_pct,
  TRY_CAST(rec.promo.valid_until AS DATE) AS promo_valid_until,
  rec._source_file AS source_file
FROM latest;

-- ============================================================================
-- DEMO EXTENSION 2: UNSTRUCTURED SOURCE (PRODUCT IMAGES + PDF CATALOGS)
-- Binary files land in /Volumes/ops/bronze/ingest/product_docs/ and are
-- ingested as raw bytes; ai_parse_document (an AI function) then turns them
-- into structured, queryable output inside the pipeline.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE ops.bronze.product_docs_raw
COMMENT 'Bronze: Raw unstructured product documents (PDF catalogs, product images) ingested as binary files'
AS
SELECT
  *,
  _metadata.file_name AS _source_file,
  _metadata.file_modification_time AS _source_file_mtime
FROM STREAM read_files(
  '/Volumes/ops/bronze/ingest/product_docs/',
  format => 'binaryFile',
  pathGlobFilter => '*.{pdf,png,jpg,jpeg}'
);

CREATE OR REFRESH MATERIALIZED VIEW ops.silver.product_docs_parsed
COMMENT 'Silver: Unstructured product documents parsed by the ai_parse_document AI function into structured, queryable VARIANT output'
AS
SELECT
  path AS doc_path,
  _source_file AS source_file,
  CASE
    WHEN path LIKE '%.pdf' THEN 'PDF catalog'
    WHEN path LIKE '%.png' THEN 'Product image'
    ELSE 'Other document'
  END AS doc_type,
  length AS size_bytes,
  content AS raw_content,
  ai_parse_document(content, MAP('version', '2.0')) AS parsed_doc
FROM ops.bronze.product_docs_raw;

-- ============================================================================
-- DEMO EXTENSION 3: GOLD LAYER TIE-INS
-- The new sources feed the same governed Gold layer as the structured ones.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ops.gold.product_performance
COMMENT 'Gold: Product revenue analytics joining the semi-structured JSON catalog with transaction facts'
AS
WITH tx AS (
  SELECT
    product_id,
    SUM(quantity) AS units_sold,
    SUM(amount_usd) AS revenue_usd,
    COUNT(DISTINCT transaction_id) AS orders_count
  FROM ops.silver.customer_transactions
  GROUP BY product_id
)
SELECT
  c.product_id,
  c.product_name,
  c.product_category,
  c.brand,
  c.list_price_usd,
  c.product_status,
  COALESCE(t.units_sold, 0) AS units_sold,
  COALESCE(t.orders_count, 0) AS orders_count,
  COALESCE(t.revenue_usd, 0) AS realized_revenue_usd,
  CASE
    WHEN COALESCE(t.orders_count, 0) = 0 THEN 'Catalog-only (no sales yet)'
    WHEN t.revenue_usd >= 5000 THEN 'Top seller'
    ELSE 'Selling'
  END AS sales_performance
FROM ops.silver.product_catalog_cleaned c
LEFT JOIN tx t ON c.product_id = t.product_id;

CREATE OR REFRESH MATERIALIZED VIEW ops.gold.document_intelligence_summary
COMMENT 'Gold: Unstructured document intelligence - images and PDF catalogs made queryable by ai_parse_document'
AS
SELECT
  source_file,
  doc_type,
  size_bytes,
  cardinality(try_cast(parsed_doc:document:pages AS ARRAY<VARIANT>)) AS page_count,
  cardinality(try_cast(parsed_doc:document:elements AS ARRAY<VARIANT>)) AS element_count,
  length(concat_ws('\n', transform(
    try_cast(parsed_doc:document:elements AS ARRAY<VARIANT>),
    e -> try_cast(e:content AS STRING)
  ))) AS extracted_text_chars
FROM ops.silver.product_docs_parsed
WHERE is_variant_null(parsed_doc:error_status);