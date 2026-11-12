# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Automated Pipeline Deployment & 2-Hour Scheduler (`databricks-sdk`)
# MAGIC Run this notebook (`Run all`) after Step 1 to automatically create:
# MAGIC 1. **Lakeflow Declarative Pipeline (Delta Live Tables)** transforming `ops.bronze` -> `ops.silver` -> `ops.gold`.
# MAGIC 2. **Databricks Workflow Job ("Run every 2 hours")** orchestrating the Medallion pipeline on a 2-hour schedule (`0 0 */2 * * ?`).

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines, jobs

w = WorkspaceClient()
current_user = w.current_user.me().user_name
workspace_folder = f"/Users/{current_user}"

dlt_notebook_path = f"{workspace_folder}/02_pipeline_dlt_bronze_to_gold"
merge_notebook_path = f"{workspace_folder}/03_pipeline_sql_merge_bronze_to_gold"

print(f"Deploying pipelines for user: {current_user}")

# 1. Create Lakeflow Declarative Pipeline (Delta Live Tables)
try:
    dlt_pipeline = w.pipelines.create(
        name="OPS Medallion Pipeline (Bronze -> Silver -> Gold)",
        catalog="ops",
        target="silver",
        serverless=True,
        continuous=False,
        libraries=[
            pipelines.PipelineLibrary(
                notebook=pipelines.NotebookLibrary(path=dlt_notebook_path)
            )
        ]
    )
    pipeline_id = dlt_pipeline.pipeline_id
    print(f"Created Lakeflow Declarative Pipeline! ID: {pipeline_id}")
except Exception as e:
    pipeline_id = None
    print(f"DLT Pipeline creation note: {e}")

# 2. Create Scheduled Databricks Workflow Job ("Run every 2 hours")
warehouses = list(w.warehouses.list())
warehouse_id = warehouses[0].id if warehouses else None

tasks = []
if pipeline_id:
    tasks.append(
        jobs.Task(
            task_key="run_medallion_dlt_pipeline",
            description="Transform Bronze -> Silver -> Gold via Lakeflow Declarative Pipeline",
            pipeline_task=jobs.PipelineTask(pipeline_id=pipeline_id, full_refresh=False)
        )
    )
elif warehouse_id:
    tasks.append(
        jobs.Task(
            task_key="bronze_to_silver_to_gold_merge",
            description="Incremental SQL MERGE pipeline from Bronze -> Silver -> Gold",
            notebook_task=jobs.NotebookTask(notebook_path=merge_notebook_path),
            warehouse_id=warehouse_id
        )
    )

if tasks:
    created_job = w.jobs.create(
        name="OPS Medallion Data Pipeline (Every 2 Hours)",
        tasks=tasks,
        schedule=jobs.CronSchedule(
            quartz_cron_expression="0 0 */2 * * ?",
            timezone_id="UTC",
            pause_status=jobs.PauseStatus.UNPAUSED
        )
    )
    print(f"Created Scheduled Workflow Job! Job ID: {created_job.job_id}")
    print("Schedule: Runs automatically every 2 hours (0 0 */2 * * ? UTC)")
else:
    print("Ensure 02_pipeline_dlt_bronze_to_gold is imported in your user folder or an active SQL Warehouse exists.")
