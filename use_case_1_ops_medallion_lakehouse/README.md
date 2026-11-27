# OPS Modern Data & AI Lakehouse — Complete Package

A single, ready-to-import Databricks package implementing an end-to-end **Medallion Architecture (`Bronze` → `Silver` → `Gold`)**, automated **2-hour transformation pipelines**, **AI/ML product recommendations**, **bilingual marketing campaigns**, **SQL alerts**, and an **AI/BI Executive Dashboard**.

---

## 📦 What's Inside This Package

| Order | File Name | Purpose |
| :--- | :--- | :--- |
| **Start Here** | `README.md` | Project overview and step-by-step setup guide. |
| **Step 1** | `01_setup_catalog_tables_and_sample_data.sql` | Creates the `ops` Unity Catalog, `bronze`/`silver`/`gold` schemas, all 16 tables, and loads sample data (Cloud Storage, Kafka, Salesforce). |
| **Step 2A** | `02_pipeline_dlt_bronze_to_gold.sql` | **Option A (Recommended):** Lakeflow Declarative Pipeline (Delta Live Tables) with streaming & data quality rules (`Bronze` → `Silver` → `Gold`). |
| **Step 2B** | `03_pipeline_sql_merge_bronze_to_gold.sql` | **Option B (Standard SQL):** Idempotent `MERGE INTO` pipeline for standard SQL Warehouses / Workflow Jobs (no DLT required). |
| **Step 3** | `04_deploy_and_schedule_pipeline.py` | Automatically deploys the pipeline and schedules it to run **every 2 hours** using the Databricks SDK. |
| **Step 4** | `05_ai_ml_marketing_alerts_dashboard.py` | Runs end-user use cases on Gold data: **MLflow Product Recommender**, **Marketing Campaigns (EN/AR)**, **SQL Alerts**, and **AI/BI Lakeview Dashboard**. |
| *(Optional)* | `databricks.yml` | Databricks Asset Bundle (DAB) config if you prefer deploying via Databricks CLI (`databricks bundle deploy`). |

---

## 🚀 Simple 4-Step Setup Instructions

### 1. Import This ZIP File into Databricks
1. Log into your **Databricks Workspace**.
2. Click **Workspace** in the left sidebar → navigate to your user folder (`/Users/<your-email>/`).
3. Click the **⋮** menu (or **Import**) → select **`databricks_ops_lakehouse_complete_package.zip`** → click **Import**.
   *(Databricks automatically extracts all notebooks into your folder.)*

### 2. Create Tables & Load Sample Data
1. Open **`01_setup_catalog_tables_and_sample_data`**.
2. Attach a **Compute Cluster** or **SQL Warehouse** (top right).
3. Click **Run all**.
   *(This creates `ops.bronze.*`, `ops.silver.*`, and `ops.gold.*` with initial sample records.)*

### 3. Set Up & Schedule the Transformation Pipeline (Every 2 Hours)
1. Open **`04_deploy_and_schedule_pipeline`**.
2. Attach a **Compute Cluster** and click **Run all**.
   *(This automatically creates the **OPS Medallion Pipeline** and schedules a Databricks Workflow Job to refresh `Bronze -> Silver -> Gold` every 2 hours.)*
   - *Manual alternative:* Go to **Workflows → Pipelines → Create Pipeline**, point it to `02_pipeline_dlt_bronze_to_gold`, set Catalog = `ops` and Target Schema = `silver`, and click **Start**.

### 4. Run the AI Recommender, Marketing Campaigns, Alerts & Dashboard
1. Open **`05_ai_ml_marketing_alerts_dashboard`**.
2. Attach a **Compute Cluster** (DBR 14.3 LTS or newer) and click **Run all**.
3. Once finished, check your Workspace for:
   - **ML Product Recommendations:** `ops.gold.product_recommendations` (and experiment `/Shared/ops_ai_product_recommender`)
   - **Marketing Target List (EN & AR):** `ops.gold.marketing_campaign_targets`
   - **SQL Alerts:** Under **SQL → Alerts** (`[OPS Alert] High-Value Customer Churn Risk` & `Stalled Enterprise Opportunity`)
   - **Executive Dashboard:** Under **Dashboards** (`OPS Lakehouse - Executive AI, Marketing & Revenue Dashboard`)
