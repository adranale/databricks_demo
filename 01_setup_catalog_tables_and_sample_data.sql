-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Step 1: Create Unity Catalog, Medallion Tables & Sample Data
-- MAGIC **Catalog:** `ops` | **Schemas:** `bronze`, `silver`, `gold`
-- MAGIC
-- MAGIC Run this notebook first (`Run all`) to create all 16 tables across the Medallion Architecture and populate initial sample data.

-- COMMAND ----------

-- ============================================================================
-- STEP 0: CREATE UNITY CATALOG & MEDALLION SCHEMAS
-- ============================================================================
CREATE CATALOG IF NOT EXISTS ops
COMMENT 'Enterprise Operations & AI Data Platform Catalog';

USE CATALOG ops;

CREATE SCHEMA IF NOT EXISTS ops.bronze
COMMENT 'Bronze Layer: Raw, append-only ingestion from Cloud Storage, Kafka, and Salesforce';

CREATE SCHEMA IF NOT EXISTS ops.silver
COMMENT 'Silver Layer: Cleansed, deduplicated, conformed Single Source of Truth';

CREATE SCHEMA IF NOT EXISTS ops.gold
COMMENT 'Gold Layer: Curated business KPIs, BI aggregates, and AI/ML feature tables';

-- COMMAND ----------

-- ============================================================================
-- STEP 1: BRONZE LAYER (RAW TABLES & SAMPLE INGESTION DATA)
-- ============================================================================

-- 1.1 Cloud Storage -> ops.bronze.customers_raw
CREATE OR REPLACE TABLE ops.bronze.customers_raw (
    raw_json STRING COMMENT 'Raw JSON customer profile landed from Cloud Storage (S3/ADLS)',
    _ingest_timestamp TIMESTAMP COMMENT 'Ingestion timestamp into Bronze',
    _source_system STRING COMMENT 'Source identifier',
    _source_file STRING COMMENT 'Landing file path'
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Raw customer records ingested from Cloud Storage';

INSERT INTO ops.bronze.customers_raw VALUES
('{"customer_id":"C101","full_name":"  ahmed al-mansoor ","email":"AHMED.MANSOOR@TECHCORP.SA","city":"riyadh","country":"Saudi Arabia","segment":"Enterprise","account_id":"ACC-01","signup_date":"2024-01-15"}', TIMESTAMP '2026-03-01 08:00:00', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json'),
('{"customer_id":"C102","full_name":"Sara Al-Otaibi","email":"sara.otaibi@finserve.ae","city":"Dubai","country":"UAE","segment":"Enterprise","account_id":"ACC-02","signup_date":"2024-02-10"}', TIMESTAMP '2026-03-01 08:00:05', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json'),
('{"customer_id":"C103","full_name":"omar hassan","email":"omar.h@retailhub.eg","city":"cairo","country":"Egypt","segment":"Mid-Market","account_id":"ACC-03","signup_date":"2024-03-22"}', TIMESTAMP '2026-03-01 08:00:10', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json'),
-- Intentional duplicate record to demonstrate Silver deduplication
('{"customer_id":"C103","full_name":"Omar Hassan","email":"omar.hassan@retailhub.eg","city":"Cairo","country":"Egypt","segment":"Mid-Market","account_id":"ACC-03","signup_date":"2024-03-22"}', TIMESTAMP '2026-03-01 09:15:00', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_02.json'),
('{"customer_id":"C104","full_name":"Layla Mahmoud","email":"layla@medhealth.sa","city":"Jeddah","country":"Saudi Arabia","segment":"Enterprise","account_id":"ACC-04","signup_date":"2024-04-05"}', TIMESTAMP '2026-03-01 08:00:15', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json'),
('{"customer_id":"C105","full_name":"tariq al-zahrani","email":null,"city":"dammam","country":"Saudi Arabia","segment":"SMB","account_id":"ACC-05","signup_date":"2024-05-18"}', TIMESTAMP '2026-03-01 08:00:20', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json'),
('{"customer_id":"C106","full_name":"Nadia Al-Kuwari","email":"nadia@qatarenergy-logistics.qa","city":"Doha","country":"Qatar","segment":"Enterprise","account_id":"ACC-06","signup_date":"2024-06-11"}', TIMESTAMP '2026-03-01 08:00:25', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json'),
('{"customer_id":"C107","full_name":"Khalid Bin Salman","email":"khalid@riyadhbank-digital.sa","city":"Riyadh","country":"Saudi Arabia","segment":"Enterprise","account_id":"ACC-01","signup_date":"2024-07-02"}', TIMESTAMP '2026-03-01 08:00:30', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json'),
('{"customer_id":"C108","full_name":"Mona Al-Harbi","email":"mona@gulfairways.bh","city":"Manama","country":"Bahrain","segment":"Mid-Market","account_id":"ACC-07","signup_date":"2024-08-19"}', TIMESTAMP '2026-03-01 08:00:35', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json'),
('{"customer_id":"C109","full_name":"Youssef El-Masry","email":"youssef@alexandria-ports.eg","city":"Alexandria","country":"Egypt","segment":"Mid-Market","account_id":"ACC-08","signup_date":"2024-09-14"}', TIMESTAMP '2026-03-01 08:00:40', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json'),
('{"customer_id":"C110","full_name":"Fatima Al-Suwaidi","email":"fatima@abudhabi-smart.ae","city":"Abu Dhabi","country":"UAE","segment":"Enterprise","account_id":"ACC-02","signup_date":"2024-10-01"}', TIMESTAMP '2026-03-01 08:00:45', 'CloudStorage', 's3://ops-landing/customers/2026/03/01/cust_01.json');

-- 1.2 Kafka -> ops.bronze.transactions_raw
CREATE OR REPLACE TABLE ops.bronze.transactions_raw (
    kafka_key STRING COMMENT 'Kafka partition key (customer_id)',
    raw_payload STRING COMMENT 'Raw JSON event payload from Kafka topic ops.pos.transactions',
    kafka_topic STRING COMMENT 'Source Kafka topic',
    kafka_partition INT COMMENT 'Kafka partition number',
    kafka_offset BIGINT COMMENT 'Kafka message offset',
    _ingest_timestamp TIMESTAMP COMMENT 'Streaming ingestion timestamp'
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Raw real-time transaction events streamed from Kafka';

INSERT INTO ops.bronze.transactions_raw VALUES
('C101', '{"tx_id":"TX9001","c_id":"C101","prod_id":"P01","prod_name":"Cloud Data Warehouse Pro","category":"Data Platform","qty":1,"unit_price":1200.00,"amt":1200.00,"curr":"USD","channel":"Online","store":"Riyadh Digital Hub","ts":"2026-01-10T10:15:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1001, TIMESTAMP '2026-01-10 10:15:02'),
('C101', '{"tx_id":"TX9002","c_id":"C101","prod_id":"P02","prod_name":"AI Governance & Lineage Pack","category":"Governance","qty":1,"unit_price":850.00,"amt":850.00,"curr":"USD","channel":"Online","store":"Riyadh Digital Hub","ts":"2026-01-22T14:30:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1002, TIMESTAMP '2026-01-22 14:30:01'),
-- Intentional duplicate Kafka event (at-least-once delivery)
('C101', '{"tx_id":"TX9002","c_id":"C101","prod_id":"P02","prod_name":"AI Governance & Lineage Pack","category":"Governance","qty":1,"unit_price":850.00,"amt":850.00,"curr":"USD","channel":"Online","store":"Riyadh Digital Hub","ts":"2026-01-22T14:30:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1003, TIMESTAMP '2026-01-22 14:30:05'),
('C101', '{"tx_id":"TX9003","c_id":"C101","prod_id":"P03","prod_name":"Real-Time Streaming Connector","category":"Data Engineering","qty":2,"unit_price":450.00,"amt":900.00,"curr":"USD","channel":"Direct Sales","store":"Riyadh Branch #4","ts":"2026-02-05T09:20:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1004, TIMESTAMP '2026-02-05 09:20:02'),
('C101', '{"tx_id":"TX9012","c_id":"C101","prod_id":"P04","prod_name":"Vector Search & RAG Engine","category":"GenAI","qty":1,"unit_price":500.00,"amt":500.00,"curr":"USD","channel":"Online","store":"Riyadh Digital Hub","ts":"2026-03-01T14:32:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1005, TIMESTAMP '2026-03-01 14:32:01'),

('C102', '{"tx_id":"TX9004","c_id":"C102","prod_id":"P01","prod_name":"Cloud Data Warehouse Pro","category":"Data Platform","qty":2,"unit_price":1200.00,"amt":2400.00,"curr":"USD","channel":"Direct Sales","store":"Dubai DIFC Office","ts":"2026-01-14T11:00:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 1, 2001, TIMESTAMP '2026-01-14 11:00:03'),
('C102', '{"tx_id":"TX9005","c_id":"C102","prod_id":"P02","prod_name":"AI Governance & Lineage Pack","category":"Governance","qty":1,"unit_price":850.00,"amt":850.00,"curr":"USD","channel":"Online","store":"Dubai DIFC Office","ts":"2026-02-11T16:45:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 1, 2002, TIMESTAMP '2026-02-11 16:45:02'),
('C102', '{"tx_id":"TX9006","c_id":"C102","prod_id":"P05","prod_name":"MLflow Enterprise Feature Store","category":"Machine Learning","qty":1,"unit_price":1100.00,"amt":1100.00,"curr":"USD","channel":"Direct Sales","store":"Dubai DIFC Office","ts":"2026-02-28T13:10:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 1, 2003, TIMESTAMP '2026-02-28 13:10:01'),

('C103', '{"tx_id":"TX9007","c_id":"C103","prod_id":"P03","prod_name":"Real-Time Streaming Connector","category":"Data Engineering","qty":1,"unit_price":450.00,"amt":450.00,"curr":"USD","channel":"Online","store":"Cairo Smart Village","ts":"2026-01-18T12:00:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 2, 3001, TIMESTAMP '2026-01-18 12:00:02'),
('C103', '{"tx_id":"TX9008","c_id":"C103","prod_id":"P06","prod_name":"BI Executive Dashboard Suite","category":"Analytics","qty":2,"unit_price":300.00,"amt":600.00,"curr":"USD","channel":"Online","store":"Cairo Smart Village","ts":"2026-02-15T15:20:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 2, 3002, TIMESTAMP '2026-02-15 15:20:01'),

('C104', '{"tx_id":"TX9009","c_id":"C104","prod_id":"P01","prod_name":"Cloud Data Warehouse Pro","category":"Data Platform","qty":1,"unit_price":1200.00,"amt":1200.00,"curr":"USD","channel":"Direct Sales","store":"Jeddah Tech Park","ts":"2026-01-08T09:00:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1006, TIMESTAMP '2026-01-08 09:00:02'),
('C104', '{"tx_id":"TX9010","c_id":"C104","prod_id":"P04","prod_name":"Vector Search & RAG Engine","category":"GenAI","qty":2,"unit_price":500.00,"amt":1000.00,"curr":"USD","channel":"Online","store":"Jeddah Tech Park","ts":"2026-02-20T17:30:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1007, TIMESTAMP '2026-02-20 17:30:02'),
('C104', '{"tx_id":"TX9011","c_id":"C104","prod_id":"P05","prod_name":"MLflow Enterprise Feature Store","category":"Machine Learning","qty":1,"unit_price":1100.00,"amt":1100.00,"curr":"USD","channel":"Online","store":"Jeddah Tech Park","ts":"2026-03-02T11:05:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1008, TIMESTAMP '2026-03-02 11:05:01'),

('C105', '{"tx_id":"TX9013","c_id":"C105","prod_id":"P06","prod_name":"BI Executive Dashboard Suite","category":"Analytics","qty":1,"unit_price":300.00,"amt":300.00,"curr":"USD","channel":"Online","store":"Dammam Hub","ts":"2025-11-12T10:00:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 1, 2004, TIMESTAMP '2025-11-12 10:00:02'),

('C106', '{"tx_id":"TX9014","c_id":"C106","prod_id":"P01","prod_name":"Cloud Data Warehouse Pro","category":"Data Platform","qty":3,"unit_price":1200.00,"amt":3600.00,"curr":"USD","channel":"Direct Sales","store":"Doha West Bay","ts":"2026-01-25T14:00:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 2, 3003, TIMESTAMP '2026-01-25 14:00:01'),
('C106', '{"tx_id":"TX9015","c_id":"C106","prod_id":"P02","prod_name":"AI Governance & Lineage Pack","category":"Governance","qty":2,"unit_price":850.00,"amt":1700.00,"curr":"USD","channel":"Direct Sales","store":"Doha West Bay","ts":"2026-02-18T16:10:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 2, 3004, TIMESTAMP '2026-02-18 16:10:01'),
('C106', '{"tx_id":"TX9016","c_id":"C106","prod_id":"P07","prod_name":"Automated FinOps Cost Optimizer","category":"Cloud Ops","qty":1,"unit_price":650.00,"amt":650.00,"curr":"USD","channel":"Online","store":"Doha West Bay","ts":"2026-03-03T09:45:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 2, 3005, TIMESTAMP '2026-03-03 09:45:01'),

('C107', '{"tx_id":"TX9017","c_id":"C107","prod_id":"P01","prod_name":"Cloud Data Warehouse Pro","category":"Data Platform","qty":1,"unit_price":1200.00,"amt":1200.00,"curr":"USD","channel":"Online","store":"Riyadh Digital Hub","ts":"2026-02-01T12:15:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1009, TIMESTAMP '2026-02-01 12:15:02'),
('C107', '{"tx_id":"TX9018","c_id":"C107","prod_id":"P04","prod_name":"Vector Search & RAG Engine","category":"GenAI","qty":1,"unit_price":500.00,"amt":500.00,"curr":"USD","channel":"Online","store":"Riyadh Digital Hub","ts":"2026-02-25T18:00:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 0, 1010, TIMESTAMP '2026-02-25 18:00:01'),

('C108', '{"tx_id":"TX9019","c_id":"C108","prod_id":"P03","prod_name":"Real-Time Streaming Connector","category":"Data Engineering","qty":2,"unit_price":450.00,"amt":900.00,"curr":"USD","channel":"Online","store":"Manama Bay","ts":"2025-12-10T11:30:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 1, 2005, TIMESTAMP '2025-12-10 11:30:01'),
('C108', '{"tx_id":"TX9020","c_id":"C108","prod_id":"P07","prod_name":"Automated FinOps Cost Optimizer","category":"Cloud Ops","qty":1,"unit_price":650.00,"amt":650.00,"curr":"USD","channel":"Online","store":"Manama Bay","ts":"2026-01-19T10:20:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 1, 2006, TIMESTAMP '2026-01-19 10:20:01'),

('C109', '{"tx_id":"TX9021","c_id":"C109","prod_id":"P06","prod_name":"BI Executive Dashboard Suite","category":"Analytics","qty":1,"unit_price":300.00,"amt":300.00,"curr":"USD","channel":"Online","store":"Alexandria Port","ts":"2026-02-09T14:10:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 2, 3006, TIMESTAMP '2026-02-09 14:10:02'),
('C109', '{"tx_id":"TX9022","c_id":"C109","prod_id":"P08","prod_name":"Data Quality Observability Agent","category":"Governance","qty":1,"unit_price":550.00,"amt":550.00,"curr":"USD","channel":"Online","store":"Alexandria Port","ts":"2026-03-04T15:50:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 2, 3007, TIMESTAMP '2026-03-04 15:50:01'),

('C110', '{"tx_id":"TX9023","c_id":"C110","prod_id":"P01","prod_name":"Cloud Data Warehouse Pro","category":"Data Platform","qty":2,"unit_price":1200.00,"amt":2400.00,"curr":"USD","channel":"Direct Sales","store":"Abu Dhabi Corniche","ts":"2026-01-30T09:40:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 1, 2007, TIMESTAMP '2026-01-30 09:40:01'),
('C110', '{"tx_id":"TX9024","c_id":"C110","prod_id":"P05","prod_name":"MLflow Enterprise Feature Store","category":"Machine Learning","qty":1,"unit_price":1100.00,"amt":1100.00,"curr":"USD","channel":"Direct Sales","store":"Abu Dhabi Corniche","ts":"2026-02-27T16:00:00Z","st":"COMPLETED"}', 'ops.pos.transactions', 1, 2008, TIMESTAMP '2026-02-27 16:00:02'),
-- Intentional cancelled transaction to filter in Silver
('C110', '{"tx_id":"TX9025","c_id":"C110","prod_id":"P04","prod_name":"Vector Search & RAG Engine","category":"GenAI","qty":1,"unit_price":500.00,"amt":500.00,"curr":"USD","channel":"Online","store":"Abu Dhabi Corniche","ts":"2026-03-01T16:05:00Z","st":"CANCELLED"}', 'ops.pos.transactions', 1, 2009, TIMESTAMP '2026-03-01 16:05:01');

-- 1.3 Salesforce -> ops.bronze.accounts_raw
CREATE OR REPLACE TABLE ops.bronze.accounts_raw (
    sf_account_id STRING,
    account_name STRING,
    industry STRING,
    annual_revenue_raw STRING,
    account_tier STRING,
    region STRING,
    owner_name STRING,
    _ingest_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Raw Salesforce CRM Account records';

INSERT INTO ops.bronze.accounts_raw VALUES
('ACC-01', '  TechCorp Saudi Arabia  ', 'Technology & Banking', '45000000', 'Strategic', 'KSA', 'Zaid Al-Qahtani', TIMESTAMP '2026-03-01 07:00:00'),
('ACC-02', 'FinServe Gulf Holdings', 'Financial Services', '62000000', 'Strategic', 'UAE', 'Reem Al-Mansoori', TIMESTAMP '2026-03-01 07:00:00'),
('ACC-03', 'RetailHub MENA', 'Retail & E-Commerce', '18500000', 'Growth', 'Egypt', 'Karim Nabil', TIMESTAMP '2026-03-01 07:00:00'),
('ACC-04', 'MedHealth Care Group', 'Healthcare', '31000000', 'Enterprise', 'KSA', 'Zaid Al-Qahtani', TIMESTAMP '2026-03-01 07:00:00'),
('ACC-05', 'Eastern Industrial Supply', 'Manufacturing', '8200000', 'Commercial', 'KSA', 'Noura Al-Dosari', TIMESTAMP '2026-03-01 07:00:00'),
('ACC-06', 'Qatar Energy & Logistics', 'Energy & Utilities', '88000000', 'Strategic', 'Qatar', 'Reem Al-Mansoori', TIMESTAMP '2026-03-01 07:00:00'),
('ACC-07', 'Gulf Airways Digital', 'Aviation & Travel', '24000000', 'Enterprise', 'Bahrain', 'Reem Al-Mansoori', TIMESTAMP '2026-03-01 07:00:00'),
('ACC-08', 'Alexandria Maritime Ports', 'Logistics', '14500000', 'Growth', 'Egypt', 'Karim Nabil', TIMESTAMP '2026-03-01 07:00:00');

-- 1.4 Salesforce -> ops.bronze.opportunities_raw
CREATE OR REPLACE TABLE ops.bronze.opportunities_raw (
    sf_opp_id STRING,
    sf_account_id STRING,
    opp_name STRING,
    stage_name STRING,
    amount_raw STRING,
    probability_pct INT,
    close_date STRING,
    days_in_stage INT,
    _ingest_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Raw Salesforce CRM Opportunity pipeline records';

INSERT INTO ops.bronze.opportunities_raw VALUES
('OPP-501', 'ACC-01', 'Enterprise Lakehouse Expansion 2026', 'Closed Won', '120000', 100, '2026-02-15', 5, TIMESTAMP '2026-03-01 07:05:00'),
('OPP-502', 'ACC-01', 'GenAI Customer Support RAG Pilot', 'Negotiation', '85000', 75, '2026-03-28', 34, TIMESTAMP '2026-03-01 07:05:00'),
('OPP-503', 'ACC-02', 'Real-Time Fraud Detection Streaming', 'Closed Won', '150000', 100, '2026-02-20', 3, TIMESTAMP '2026-03-01 07:05:00'),
('OPP-504', 'ACC-02', 'Unity Catalog Multi-Region Governance', 'Proposal', '65000', 60, '2026-04-15', 14, TIMESTAMP '2026-03-01 07:05:00'),
('OPP-505', 'ACC-03', 'Omnichannel Personalization Engine', 'Negotiation', '55000', 70, '2026-03-30', 38, TIMESTAMP '2026-03-01 07:05:00'),
('OPP-506', 'ACC-04', 'Clinical Data Lakehouse Migration', 'Closed Won', '95000', 100, '2026-01-29', 4, TIMESTAMP '2026-03-01 07:05:00'),
('OPP-507', 'ACC-05', 'ERP Reporting Modernization', 'Closed Lost', '25000', 0, '2026-02-10', 45, TIMESTAMP '2026-03-01 07:05:00'),
('OPP-508', 'ACC-06', 'Predictive Maintenance IoT Lakehouse', 'Closed Won', '210000', 100, '2026-02-25', 2, TIMESTAMP '2026-03-01 07:05:00'),
('OPP-509', 'ACC-07', 'Dynamic Pricing ML Pipeline', 'Qualification', '48000', 35, '2026-05-10', 12, TIMESTAMP '2026-03-01 07:05:00'),
('OPP-510', 'ACC-08', 'Port Supply Chain Analytics', 'Proposal', '42000', 55, '2026-04-20', 19, TIMESTAMP '2026-03-01 07:05:00');

-- 1.5 Salesforce -> ops.bronze.leads_raw
CREATE OR REPLACE TABLE ops.bronze.leads_raw (
    sf_lead_id STRING,
    lead_name STRING,
    company_name STRING,
    email STRING,
    lead_source STRING,
    status STRING,
    estimated_budget_raw STRING,
    region STRING,
    created_date STRING,
    _ingest_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Raw Salesforce CRM Marketing & Sales Leads';

INSERT INTO ops.bronze.leads_raw VALUES
('LD-801', '  salman al-ghamdi ', 'Riyadh Telecom Ventures', 'salman@rtv.sa', 'AI Summit Riyadh', 'Qualified', '90000', 'KSA', '2026-02-12', TIMESTAMP '2026-03-01 07:10:00'),
('LD-802', 'Huda Al-Maktoum', 'Emirates Smart Retail', 'huda@esretail.ae', 'Webinar - Medallion Architecture', 'Working', '60000', 'UAE', '2026-02-18', TIMESTAMP '2026-03-01 07:10:00'),
('LD-803', 'Tarek Abdel-Rahman', 'Nile FinTech Labs', 'tarek@nilefintech.eg', 'Website Demo Request', 'Qualified', '45000', 'Egypt', '2026-02-22', TIMESTAMP '2026-03-01 07:10:00'),
('LD-804', 'Aisha Al-Thani', 'Doha Health Innovations', 'aisha@dohahealth.qa', 'Partner Referral', 'New', '75000', 'Qatar', '2026-02-26', TIMESTAMP '2026-03-01 07:10:00'),
('LD-805', 'Majed Al-Otaibi', 'Najd Logistics Co', 'majed@najdlogistics.sa', 'LinkedIn Campaign', 'Working', '35000', 'KSA', '2026-02-28', TIMESTAMP '2026-03-01 07:10:00'),
('LD-806', 'Rania Khoury', 'Levant Cloud Solutions', null, 'Cold Outreach', 'Disqualified', '10000', 'UAE', '2026-01-15', TIMESTAMP '2026-03-01 07:10:00');

-- COMMAND ----------

-- ============================================================================
-- STEP 2: SILVER LAYER (CLEANSED, DEDUPLICATED & ENRICHED TABLES)
-- ============================================================================

CREATE OR REPLACE TABLE ops.silver.customers_cleaned
COMMENT 'Deduplicated and standardized customer master table'
AS
WITH ranked_customers AS (
    SELECT
        get_json_object(raw_json, '$.customer_id') AS customer_id,
        INITCAP(TRIM(get_json_object(raw_json, '$.full_name'))) AS customer_name,
        COALESCE(LOWER(TRIM(get_json_object(raw_json, '$.email'))), 'missing_email@unknown.local') AS email,
        INITCAP(TRIM(get_json_object(raw_json, '$.city'))) AS city,
        TRIM(get_json_object(raw_json, '$.country')) AS country,
        TRIM(get_json_object(raw_json, '$.segment')) AS segment,
        TRIM(get_json_object(raw_json, '$.account_id')) AS account_id,
        CAST(get_json_object(raw_json, '$.signup_date') AS DATE) AS signup_date,
        _ingest_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY get_json_object(raw_json, '$.customer_id')
            ORDER BY _ingest_timestamp DESC
        ) AS rn
    FROM ops.bronze.customers_raw
    WHERE get_json_object(raw_json, '$.customer_id') IS NOT NULL
)
SELECT
    customer_id,
    customer_name,
    email,
    city,
    country,
    segment,
    account_id,
    signup_date,
    _ingest_timestamp AS last_updated_ts
FROM ranked_customers
WHERE rn = 1;

CREATE OR REPLACE TABLE ops.silver.transactions_cleaned
COMMENT 'Deduplicated valid completed transactions parsed from Kafka raw payloads'
AS
WITH parsed_tx AS (
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
        UPPER(TRIM(get_json_object(raw_payload, '$.st'))) AS status,
        _ingest_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY get_json_object(raw_payload, '$.tx_id')
            ORDER BY _ingest_timestamp ASC
        ) AS rn
    FROM ops.bronze.transactions_raw
    WHERE raw_payload IS NOT NULL
)
SELECT
    transaction_id,
    customer_id,
    product_id,
    product_name,
    product_category,
    quantity,
    unit_price_usd,
    amount_usd,
    currency,
    sales_channel,
    store_location,
    transaction_ts,
    DATE(transaction_ts) AS transaction_date,
    status
FROM parsed_tx
WHERE rn = 1
  AND status = 'COMPLETED'
  AND amount_usd > 0;

CREATE OR REPLACE TABLE ops.silver.customer_transactions
COMMENT 'Enriched customer transaction history joining customers_cleaned and transactions_cleaned'
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

CREATE OR REPLACE TABLE ops.silver.accounts_cleaned
COMMENT 'Standardized enterprise accounts from Salesforce'
AS
SELECT DISTINCT
    TRIM(sf_account_id) AS account_id,
    TRIM(account_name) AS account_name,
    TRIM(industry) AS industry,
    CAST(annual_revenue_raw AS DECIMAL(15,2)) AS annual_revenue_usd,
    TRIM(account_tier) AS account_tier,
    UPPER(TRIM(region)) AS region,
    TRIM(owner_name) AS account_owner
FROM ops.bronze.accounts_raw
WHERE sf_account_id IS NOT NULL;

CREATE OR REPLACE TABLE ops.silver.opportunities_cleaned
COMMENT 'Validated Salesforce opportunities with weighted pipeline value'
AS
SELECT DISTINCT
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
WHERE sf_opp_id IS NOT NULL;

CREATE OR REPLACE TABLE ops.silver.account_opportunities
COMMENT 'Enriched B2B account pipeline joining accounts_cleaned and opportunities_cleaned'
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

CREATE OR REPLACE TABLE ops.silver.leads_cleaned
COMMENT 'Cleaned marketing and sales leads excluding disqualified records'
AS
SELECT DISTINCT
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
  AND status <> 'Disqualified';

-- COMMAND ----------

-- ============================================================================
-- STEP 3: GOLD LAYER (CURATED BUSINESS KPIS & AI FEATURE TABLES)
-- ============================================================================

CREATE OR REPLACE TABLE ops.gold.customer_spending
COMMENT 'Gold Customer 360 feature table for BI analytics, marketing segmentation, and churn ML'
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
        DATEDIFF(DATE '2026-03-05', MAX(transaction_date)) AS days_since_last_purchase,
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

CREATE OR REPLACE TABLE ops.gold.account_performance
COMMENT 'Gold B2B Account Performance combining product transactions and Salesforce CRM opportunities'
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

CREATE OR REPLACE TABLE ops.gold.sales_pipeline_summary
COMMENT 'Gold Sales Pipeline & Lead Funnel summary by Region and Stage'
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

CREATE OR REPLACE TABLE ops.gold.business_summary
COMMENT 'Executive Gold Business Summary across Regions combining Customer Spending and Account Performance'
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

-- Verify all Gold tables
SELECT 'customer_spending' AS table_name, COUNT(*) AS row_count FROM ops.gold.customer_spending
UNION ALL
SELECT 'account_performance', COUNT(*) FROM ops.gold.account_performance
UNION ALL
SELECT 'sales_pipeline_summary', COUNT(*) FROM ops.gold.sales_pipeline_summary
UNION ALL
SELECT 'business_summary', COUNT(*) FROM ops.gold.business_summary;