# Databricks Lakehouse — Medallion Use-Case Showcase

Three end-to-end data-engineering use cases on Databricks, each implementing the
**Medallion Architecture** (`Bronze` → `Silver` → `Gold`) with **Lakeflow Spark Declarative
Pipelines**, governed by **Unity Catalog**, and surfaced through **AI/BI (Lakeview)
dashboards**.

The three use cases deliberately cover different data shapes and sources — synthetic
business data, a personal social-network export, and public open data — while sharing the
same engineering pattern.

## Repository structure

```text
.
├── use_case_1_ops_medallion_lakehouse/     # Synthetic CRM/ERP-style sales demo (+ AI platform talk)
├── use_case_2_linkedin_export_medallion/    # Personal LinkedIn data-export analytics
└── use_case_3_strafzettel_koeln_medallion/  # Cologne 2015 parking-ticket open-data analytics
```

## Use case 1 — OPS Medallion Lakehouse (`use_case_1_ops_medallion_lakehouse/`)

A complete synthetic sales/CRM demo: batch and streaming sources (Cloud Storage, Kafka,
Salesforce) landing in `ops.bronze`, streaming dedup + data-quality rules into
`ops.silver`, and aggregated reporting tables in `ops.gold` — plus MLflow product
recommendations, bilingual marketing campaign targets, SQL alerts, and an executive
dashboard. Includes a Declarative Automation Bundle (`databricks.yml`), an SDK-based
deploy/schedule notebook, and a ready-to-present conference deck
(*Enabling AI Through Modern Data Platforms*). See the [use case 1 README](use_case_1_ops_medallion_lakehouse/README.md).

## Use case 2 — LinkedIn Data Export (`use_case_2_linkedin_export_medallion/`)

A personal LinkedIn data export modeled bronze → silver → gold: connections, messages,
invitations, positions, skills, and followed companies. The gold layer supports network
growth, employer/title composition, messaging activity, and career timeline analyses.

**Privacy:** no personal data is stored in this repository — only transformation logic.
The account holder's identity is even derived from the data itself rather than hardcoded.
See the [use case 2 README](use_case_2_linkedin_export_medallion/README.md).

## Use case 3 — Strafzettel Köln 2015 (`use_case_3_strafzettel_koeln_medallion/`)

The City of Cologne's 2015 parking-ticket open data (~970k tickets) modeled bronze →
silver → gold, enabling hotspot, violation-mix, plate-origin, and time-pattern analyses.

**Source data (open):** [Koeln Strafzettel 2015 on Kaggle](https://www.kaggle.com/datasets/adranale/koeln-strafzettel-2015).
See the [use case 3 README](use_case_3_strafzettel_koeln_medallion/README.md).

## The shared pattern

| Layer | Role | Implementation |
| :--- | :--- | :--- |
| **Bronze** | Raw, immutable ingestion of source files | Auto Loader streaming tables (`read_files`), string-typed columns, file metadata kept |
| **Silver** | Cleaned, deduplicated, typed business data | Materialized views; timestamp parsing per source format; derived flags/dimensions |
| **Gold** | Aggregated, analysis-ready marts | Materialized views (growth, composition, KPIs) feeding AI/BI dashboards |

## Getting started

Each use case is self-contained — open its folder, read its README for the data source
and prerequisites, then create a Lakeflow Spark Declarative Pipeline pointing at the
folder (use case 1 can alternatively be deployed with the included DAB or the deploy
notebook).
