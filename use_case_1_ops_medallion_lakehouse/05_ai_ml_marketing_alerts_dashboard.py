# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: End-User AI Recommender, Marketing Campaigns, SQL Alerts & Lakeview Dashboard
# MAGIC **Prerequisites:** Run `01_setup_catalog_tables_and_sample_data.sql` first.
# MAGIC
# MAGIC This notebook delivers 4 end-user business & AI use cases on top of `ops.gold`:
# MAGIC 1. **AI/ML Product Recommendation Engine (MLflow + Collaborative Filtering)** -> `ops.gold.product_recommendations`
# MAGIC 2. **Marketing Campaign Segmentation & Bilingual Copy (EN/AR)** -> `ops.gold.marketing_campaign_targets`
# MAGIC 3. **Automated Databricks SQL Alerts** (Churn Risk & Stalled Deals)
# MAGIC 4. **Programmatic AI/BI (Lakeview) Executive Dashboard**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 1: AI/ML Product Recommendation Engine (`ops.gold.product_recommendations`)

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.ensemble import RandomForestClassifier
import mlflow
import mlflow.sklearn

tx_df = spark.table("ops.silver.customer_transactions").toPandas()
cust_df = spark.table("ops.gold.customer_spending").toPandas()

products_catalog = (
    tx_df[["product_id", "product_name", "product_category", "unit_price_usd"]]
    .drop_duplicates(subset=["product_id"])
    .copy()
)
products_catalog["unit_price_usd"] = products_catalog["unit_price_usd"].astype(float)

tx_df["amount_usd"] = tx_df["amount_usd"].astype(float)
user_item_matrix = tx_df.pivot_table(
    index="customer_id",
    columns="product_id",
    values="amount_usd",
    aggfunc="sum",
    fill_value=0.0
)

item_similarity = cosine_similarity(user_item_matrix.T)
item_sim_df = pd.DataFrame(
    item_similarity,
    index=user_item_matrix.columns,
    columns=user_item_matrix.columns
)

mlflow.set_experiment("/Shared/ops_ai_product_recommender")

features = cust_df[[
    "total_orders",
    "total_units_purchased",
    "avg_order_value_usd",
    "days_since_last_purchase",
    "distinct_categories_bought"
]].astype(float)

target = (
    (cust_df["total_spending_usd"].astype(float) > 2000) |
    (cust_df["distinct_categories_bought"] >= 2)
).astype(int)

with mlflow.start_run(run_name="product_affinity_and_propensity_v1") as run:
    clf = RandomForestClassifier(n_estimators=50, max_depth=4, random_state=42)
    clf.fit(features, target)
    train_acc = clf.score(features, target)

    mlflow.log_param("recommender_algorithm", "ItemItem_CosineSimilarity_Hybrid")
    mlflow.log_param("propensity_model", "RandomForestClassifier")
    mlflow.log_metric("propensity_train_accuracy", float(train_acc))
    mlflow.sklearn.log_model(clf, artifact_path="expansion_propensity_model")
    run_id = run.info.run_id
    print(f"MLflow Run Logged Successfully! Run ID: {run_id} | Accuracy: {train_acc:.2f}")

cust_df["expansion_propensity"] = clf.predict_proba(features)[:, 1]

recommendations = []
all_product_ids = list(products_catalog["product_id"])

for _, crow in cust_df.iterrows():
    cid = crow["customer_id"]
    bought_products = set(tx_df[tx_df["customer_id"] == cid]["product_id"].unique())
    unbought = [p for p in all_product_ids if p not in bought_products]

    scored_items = []
    for candidate_pid in unbought:
        if cid in user_item_matrix.index and len(bought_products) > 0:
            sim_scores = [item_sim_df.loc[candidate_pid, bp] for bp in bought_products if bp in item_sim_df.index]
            base_score = float(np.mean(sim_scores)) if sim_scores else 0.15
        else:
            base_score = 0.20

        final_score = round(min(0.99, max(0.15, (base_score * 0.65) + (float(crow["expansion_propensity"]) * 0.35))), 2)
        scored_items.append((candidate_pid, final_score))

    scored_items.sort(key=lambda x: x[1], reverse=True)

    for rank, (rec_pid, score) in enumerate(scored_items[:3], start=1):
        pinfo = products_catalog[products_catalog["product_id"] == rec_pid].iloc[0]
        bought_names = ", ".join(tx_df[tx_df["customer_id"] == cid]["product_name"].unique()[:2])
        recommendations.append({
            "customer_id": cid,
            "customer_name": crow["customer_name"],
            "account_name": crow["account_name"],
            "region": crow["region"],
            "recommendation_rank": rank,
            "recommended_product_id": rec_pid,
            "recommended_product_name": pinfo["product_name"],
            "product_category": pinfo["product_category"],
            "estimated_deal_value_usd": float(pinfo["unit_price_usd"]),
            "affinity_score": score,
            "recommendation_reason": f"High co-adoption with {bought_names} in {crow['customer_segment']} segment",
            "mlflow_run_id": run_id,
            "generated_at": datetime.utcnow()
        })

rec_spark_df = spark.createDataFrame(pd.DataFrame(recommendations))
rec_spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ops.gold.product_recommendations")

display(spark.table("ops.gold.product_recommendations").orderBy("customer_id", "recommendation_rank"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 2: Actionable Marketing Campaign Segmentation & Bilingual Copy (`ops.gold.marketing_campaign_targets`)

# COMMAND ----------

campaign_sql = """
CREATE OR REPLACE TABLE ops.gold.marketing_campaign_targets
COMMENT 'Actionable Marketing Campaign target audience with ML recommendations and bilingual copy'
AS
WITH top_rec AS (
    SELECT *
    FROM ops.gold.product_recommendations
    WHERE recommendation_rank = 1
)
SELECT
    c.customer_id AS target_id,
    'Existing Customer' AS audience_type,
    c.customer_name AS contact_name,
    c.customer_email AS email,
    c.account_name,
    c.region,
    c.rfm_segment,
    c.churn_risk_score,
    c.total_spending_usd AS historical_value_usd,
    r.recommended_product_name AS featured_offer_product,
    r.estimated_deal_value_usd AS projected_campaign_revenue_usd,
    CASE
        WHEN c.churn_risk_score >= 0.65 THEN 'CHURN_WINBACK_20PCT'
        WHEN c.rfm_segment = 'VIP Champion' THEN 'VIP_EXECUTIVE_AI_BRIEFING'
        ELSE 'NEXT_BEST_PRODUCT_CROSS_SELL'
    END AS campaign_code,
    CASE
        WHEN c.churn_risk_score >= 0.65 THEN '20% Renewal & Expansion Credit'
        WHEN c.rfm_segment = 'VIP Champion' THEN 'Complimentary GenAI Architecture Workshop'
        ELSE '15% Bundle Discount on Recommended Module'
    END AS incentive_offer,
    CONCAT(
        'Hi ', c.customer_name, ', based on your ', c.account_name,
        ' workload, unlock ', r.recommended_product_name, ' with ',
        CASE WHEN c.churn_risk_score >= 0.65 THEN '20% off.' ELSE 'priority onboarding.' END
    ) AS message_en,
    CONCAT(
        'مرحباً ', c.customer_name, '، بناءً على استخدام ', c.account_name,
        ' لمنصتنا، نرشح لك تفعيل ', r.recommended_product_name, ' لتعزيز كفاءة البيانات والذكاء الاصطناعي.'
    ) AS message_ar,
    CURRENT_TIMESTAMP() AS created_at
FROM ops.gold.customer_spending c
LEFT JOIN top_rec r ON c.customer_id = r.customer_id

UNION ALL

SELECT
    l.lead_id AS target_id,
    'CRM Prospect Lead' AS audience_type,
    l.lead_name AS contact_name,
    l.email,
    l.company_name AS account_name,
    l.region,
    l.lead_status AS rfm_segment,
    0.00 AS churn_risk_score,
    l.estimated_budget_usd AS historical_value_usd,
    'Cloud Data Warehouse Pro' AS featured_offer_product,
    l.estimated_budget_usd AS projected_campaign_revenue_usd,
    'NEW_LEAD_LAKEHOUSE_TRIAL' AS campaign_code,
    '14-Day Free Lakehouse Proof-of-Value Sandbox' AS incentive_offer,
    CONCAT('Hi ', l.lead_name, ', accelerate ', l.company_name, ' AI readiness with a guided Lakehouse sandbox.') AS message_en,
    CONCAT('مرحباً ', l.lead_name, '، ابدأ رحلة التحول نحو الذكاء الاصطناعي في ', l.company_name, ' عبر بيئة تجريبية مجانية.') AS message_ar,
    CURRENT_TIMESTAMP() AS created_at
FROM ops.silver.leads_cleaned l
"""

spark.sql(campaign_sql)
display(spark.table("ops.gold.marketing_campaign_targets"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 3: Automated Databricks SQL Alerts (via Databricks SDK)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql

w = WorkspaceClient()

warehouses = list(w.warehouses.list())
warehouse_id = warehouses[0].id if warehouses else None
print(f"Detected SQL Warehouse ID: {warehouse_id}")

alert_definitions = [
    {
        "name": "[OPS Alert] High-Value Customer Churn Risk",
        "query_text": """
            SELECT customer_id, customer_name, account_name, total_spending_usd, churn_risk_score
            FROM ops.gold.customer_spending
            WHERE churn_risk_score >= 0.65 AND total_spending_usd >= 300
            ORDER BY churn_risk_score DESC
        """,
        "column": "churn_risk_score",
        "op": dbsql.AlertOperator.GREATER_THAN_OR_EQUAL,
        "threshold": 0.65
    },
    {
        "name": "[OPS Alert] Stalled Enterprise Opportunity (>30 Days in Stage)",
        "query_text": """
            SELECT opportunity_id, account_name, opportunity_name, stage_name, amount_usd, days_in_stage
            FROM ops.silver.account_opportunities
            WHERE is_closed = 0 AND days_in_stage > 30
            ORDER BY amount_usd DESC
        """,
        "column": "days_in_stage",
        "op": dbsql.AlertOperator.GREATER_THAN,
        "threshold": 30.0
    }
]

if warehouse_id:
    for adef in alert_definitions:
        try:
            created_query = w.queries.create(
                query=dbsql.CreateQueryRequestQuery(
                    display_name=f"Query - {adef['name']}",
                    warehouse_id=warehouse_id,
                    query_text=adef["query_text"].strip(),
                    catalog="ops",
                    schema="gold"
                )
            )
            created_alert = w.alerts.create(
                alert=dbsql.CreateAlertRequestAlert(
                    display_name=adef["name"],
                    query_id=created_query.id,
                    condition=dbsql.AlertCondition(
                        op=adef["op"],
                        operand=dbsql.AlertConditionOperand(
                            column=dbsql.AlertOperandColumn(name=adef["column"])
                        ),
                        threshold=dbsql.AlertConditionThreshold(
                            value=dbsql.AlertOperandValue(double_value=adef["threshold"])
                        )
                    ),
                    seconds_to_retrigger=86400
                )
            )
            print(f"Created SQL Alert: {created_alert.display_name} (ID: {created_alert.id})")
        except Exception as e:
            print(f"Note creating alert '{adef['name']}': {e}")
else:
    print("No active SQL Warehouse detected. Start a SQL Warehouse and re-run this cell to register SQL Alerts.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case 4: Programmatic AI/BI (Lakeview) Executive Dashboard Deployment

# COMMAND ----------

import json
from databricks.sdk.service.dashboards import Dashboard

dashboard_payload = {
    "datasets": [
        {
            "name": "ds_business_summary",
            "displayName": "Regional Business Summary",
            "query": "SELECT * FROM ops.gold.business_summary ORDER BY combined_business_value_usd DESC"
        },
        {
            "name": "ds_customer_spending",
            "displayName": "Customer 360 Spending & Churn",
            "query": "SELECT * FROM ops.gold.customer_spending ORDER BY total_spending_usd DESC"
        },
        {
            "name": "ds_recommendations",
            "displayName": "AI Product Recommendations",
            "query": "SELECT * FROM ops.gold.product_recommendations WHERE recommendation_rank = 1 ORDER BY affinity_score DESC"
        },
        {
            "name": "ds_campaigns",
            "displayName": "Marketing Campaign Targets",
            "query": "SELECT campaign_code, audience_type, COUNT(*) AS targets, SUM(projected_campaign_revenue_usd) AS projected_revenue_usd FROM ops.gold.marketing_campaign_targets GROUP BY campaign_code, audience_type"
        }
    ],
    "pages": [
        {
            "name": "executive_overview",
            "displayName": "Executive Revenue, AI & Marketing Overview",
            "layout": [
                {
                    "widget": {
                        "name": "w_regional_revenue",
                        "queries": [{"name": "main_query", "query": {"datasetName": "ds_business_summary", "fields": [{"name": "region", "expression": "`region`"}, {"name": "combined_business_value_usd", "expression": "SUM(`combined_business_value_usd`)"}], "disaggregated": False}}],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "region", "scale": {"type": "categorical"}, "displayName": "Region"},
                                "y": {"fieldName": "combined_business_value_usd", "scale": {"type": "quantitative"}, "displayName": "Combined Business Value (USD)"}
                            },
                            "frame": {"showTitle": True, "title": "Total Business Value by Region (USD)"}
                        }
                    },
                    "position": {"x": 0, "y": 0, "width": 3, "height": 4}
                },
                {
                    "widget": {
                        "name": "w_campaign_pipeline",
                        "queries": [{"name": "main_query", "query": {"datasetName": "ds_campaigns", "fields": [{"name": "campaign_code", "expression": "`campaign_code`"}, {"name": "projected_revenue_usd", "expression": "SUM(`projected_revenue_usd`)"}], "disaggregated": False}}],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "campaign_code", "scale": {"type": "categorical"}, "displayName": "Campaign"},
                                "y": {"fieldName": "projected_revenue_usd", "scale": {"type": "quantitative"}, "displayName": "Projected Revenue (USD)"}
                            },
                            "frame": {"showTitle": True, "title": "Projected Revenue by AI Marketing Campaign"}
                        }
                    },
                    "position": {"x": 3, "y": 0, "width": 3, "height": 4}
                },
                {
                    "widget": {
                        "name": "w_ai_recommendations_table",
                        "queries": [{"name": "main_query", "query": {"datasetName": "ds_recommendations", "fields": [{"name": "customer_name", "expression": "`customer_name`"}, {"name": "account_name", "expression": "`account_name`"}, {"name": "recommended_product_name", "expression": "`recommended_product_name`"}, {"name": "affinity_score", "expression": "`affinity_score`"}, {"name": "estimated_deal_value_usd", "expression": "`estimated_deal_value_usd`"}], "disaggregated": True}}],
                        "spec": {
                            "version": 2,
                            "widgetType": "table",
                            "frame": {"showTitle": True, "title": "Top-1 AI Next-Best-Product Recommendations"}
                        }
                    },
                    "position": {"x": 0, "y": 4, "width": 6, "height": 5}
                }
            ]
        }
    ]
}

try:
    current_user = w.current_user.me().user_name
    parent_path = f"/Users/{current_user}"
    created_dash = w.lakeview.create(
        dashboard=Dashboard(
            display_name="OPS Lakehouse - Executive AI, Marketing & Revenue Dashboard",
            parent_path=parent_path,
            serialized_dashboard=json.dumps(dashboard_payload),
            warehouse_id=warehouse_id
        )
    )
    print("Lakeview AI/BI Dashboard Created Successfully!")
    print(f"Dashboard ID: {created_dash.dashboard_id}")
    print(f"Workspace Path: {created_dash.path}")
except Exception as e:
    print(f"Dashboard creation info: {e}")
    print("Tip: You can also paste any dataset query from `dashboard_payload` directly into Databricks AI/BI Dashboards.")
