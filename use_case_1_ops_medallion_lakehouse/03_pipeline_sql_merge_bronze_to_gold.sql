-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Step 2B: Incremental SQL MERGE Pipeline (Bronze -> Silver -> Gold)
-- MAGIC **Catalog:** `ops` | **Alternative to DLT:** Runs on any standard SQL Warehouse or Compute Cluster.

-- COMMAND ----------

USE CATALOG ops;

-- 1.1 MERGE ops.bronze.customers_raw -> ops.silver.customers_cleaned
MERGE INTO ops.silver.customers_cleaned AS target
USING (
    SELECT
        get_json_object(raw_json, '$.customer_id') AS customer_id,
        INITCAP(TRIM(get_json_object(raw_json, '$.full_name'))) AS customer_name,
        COALESCE(LOWER(TRIM(get_json_object(raw_json, '$.email'))), 'missing_email@unknown.local') AS email,
        INITCAP(TRIM(get_json_object(raw_json, '$.city'))) AS city,
        TRIM(get_json_object(raw_json, '$.country')) AS country,
        TRIM(get_json_object(raw_json, '$.segment')) AS segment,
        TRIM(get_json_object(raw_json, '$.account_id')) AS account_id,
        CAST(get_json_object(raw_json, '$.signup_date') AS DATE) AS signup_date,
        _ingest_timestamp AS last_updated_ts
    FROM ops.bronze.customers_raw
    WHERE get_json_object(raw_json, '$.customer_id') IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY get_json_object(raw_json, '$.customer_id')
        ORDER BY _ingest_timestamp DESC
    ) = 1
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.last_updated_ts > target.last_updated_ts THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;

-- 1.2 MERGE ops.bronze.transactions_raw -> ops.silver.transactions_cleaned
MERGE INTO ops.silver.transactions_cleaned AS target
USING (
    SELECT
        get_json_object(raw_payload, '$.tx_id') AS transaction_id,
        get_json_object(raw_payload, '$.c_id') AS customer_id,
        get_json_object(raw_payload, '$.prod_id') AS product_id,
        TRIM(get_json_object(raw_payload, '$.prod_name')) AS product_name,
        TRIM(get_json_object(raw_payload, '$.category')) AS product_category,
        CAST(get_json_object(raw_payload, '$.qty') AS INT) AS quantity,
        CAST(get_json_object(raw_payload, '$.unit_price') AS DECIMAL(10,2)) AS unit_price_usd,
        CAST(get_json_object(raw_payload, '$.amt') AS DECIMAL(12,2)) AS amount_usd,
        UPPER(TRIM(get_json_object(raw_payload, '$.curr'))) AS currency,
        TRIM(get_json_object(raw_payload, '$.channel')) AS sales_channel,
        TRIM(get_json_object(raw_payload, '$.store')) AS store_location,
        CAST(get_json_object(raw_payload, '$.ts') AS TIMESTAMP) AS transaction_ts,
        DATE(CAST(get_json_object(raw_payload, '$.ts') AS TIMESTAMP)) AS transaction_date,
        UPPER(TRIM(get_json_object(raw_payload, '$.st'))) AS status
    FROM ops.bronze.transactions_raw
    WHERE raw_payload IS NOT NULL
      AND UPPER(TRIM(get_json_object(raw_payload, '$.st'))) = 'COMPLETED'
      AND CAST(get_json_object(raw_payload, '$.amt') AS DECIMAL(12,2)) > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY get_json_object(raw_payload, '$.tx_id')
        ORDER BY _ingest_timestamp ASC
    ) = 1
) AS source
ON target.transaction_id = source.transaction_id
WHEN NOT MATCHED THEN
    INSERT *;

-- 1.3 MERGE ops.bronze.accounts_raw -> ops.silver.accounts_cleaned
MERGE INTO ops.silver.accounts_cleaned AS target
USING (
    SELECT
        TRIM(sf_account_id) AS account_id,
        TRIM(account_name) AS account_name,
        TRIM(industry) AS industry,
        CAST(annual_revenue_raw AS DECIMAL(15,2)) AS annual_revenue_usd,
        TRIM(account_tier) AS account_tier,
        UPPER(TRIM(region)) AS region,
        TRIM(owner_name) AS account_owner
    FROM ops.bronze.accounts_raw
    WHERE sf_account_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(sf_account_id) ORDER BY _ingest_timestamp DESC) = 1
) AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;

-- 1.4 MERGE ops.bronze.opportunities_raw -> ops.silver.opportunities_cleaned
MERGE INTO ops.silver.opportunities_cleaned AS target
USING (
    SELECT
        TRIM(sf_opp_id) AS opportunity_id,
        TRIM(sf_account_id) AS account_id,
        TRIM(opp_name) AS opportunity_name,
        TRIM(stage_name) AS stage_name,
        CAST(amount_raw AS DECIMAL(12,2)) AS amount_usd,
        probability_pct,
        ROUND(CAST(amount_raw AS DECIMAL(12,2)) * (probability_pct / 100.0), 2) AS weighted_amount_usd,
        CAST(close_date AS DATE) AS close_date,
        days_in_stage,
        CASE WHEN stage_name = 'Closed Won' THEN 1 ELSE 0 END AS is_won,
        CASE WHEN stage_name IN ('Closed Won', 'Closed Lost') THEN 1 ELSE 0 END AS is_closed
    FROM ops.bronze.opportunities_raw
    WHERE sf_opp_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(sf_opp_id) ORDER BY _ingest_timestamp DESC) = 1
) AS source
ON target.opportunity_id = source.opportunity_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;

-- 1.5 MERGE ops.bronze.leads_raw -> ops.silver.leads_cleaned
MERGE INTO ops.silver.leads_cleaned AS target
USING (
    SELECT
        TRIM(sf_lead_id) AS lead_id,
        INITCAP(TRIM(lead_name)) AS lead_name,
        TRIM(company_name) AS company_name,
        COALESCE(LOWER(TRIM(email)), 'no_email@lead.local') AS email,
        TRIM(lead_source) AS lead_source,
        TRIM(status) AS lead_status,
        CAST(estimated_budget_raw AS DECIMAL(12,2)) AS estimated_budget_usd,
        UPPER(TRIM(region)) AS region,
        CAST(created_date AS DATE) AS created_date
    FROM ops.bronze.leads_raw
    WHERE sf_lead_id IS NOT NULL
      AND status <> 'Disqualified'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(sf_lead_id) ORDER BY _ingest_timestamp DESC) = 1
) AS source
ON target.lead_id = source.lead_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;

-- 1.6 Refresh Conformed Silver Join Tables
CREATE OR REPLACE TABLE ops.silver.customer_transactions AS
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

CREATE OR REPLACE TABLE ops.silver.account_opportunities AS
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

-- COMMAND ----------

-- ============================================================================
-- TASK 2: SILVER -> GOLD CURATED AGGREGATIONS
-- ============================================================================

CREATE OR REPLACE TABLE ops.gold.customer_spending AS
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

CREATE OR REPLACE TABLE ops.gold.account_performance AS
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

CREATE OR REPLACE TABLE ops.gold.sales_pipeline_summary AS
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

CREATE OR REPLACE TABLE ops.gold.business_summary AS
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
