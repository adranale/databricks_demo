# Use Case 2 — LinkedIn Data Export Medallion Pipeline

A **Lakeflow Spark Declarative Pipeline** that ingests a personal LinkedIn data export
(the CSV files you get from *Settings → Data privacy → Download your data*) and models it
through the Medallion Architecture in Unity Catalog (catalog: `personal`).

> **Privacy note:** No personal data (names, e-mail addresses, message contents, or any
> other information about the account holder or their network) is stored in this repository.
> The raw export lives in a private Unity Catalog volume outside git; only the
> transformation logic is versioned here. Even the "who am I" rule in the silver layer is
> derived from the data itself (see `silver/messages_cleaned.sql`), so the SQL runs
> unchanged on anyone's export.

## Pipeline layout

| Layer | Dataset | Description |
| :--- | :--- | :--- |
| Bronze | `connections_raw` | 1st-degree connections, header-renamed to snake_case, 3-line preamble skipped |
| Bronze | `messages_raw` | Full messaging history (all conversations) |
| Bronze | `invitations_raw` | Sent/received invitations |
| Bronze | `positions_raw` | Career positions |
| Bronze | `skills_raw` | Listed skills |
| Bronze | `company_follows_raw` | Followed organizations |
| Silver | `*_cleaned` (6 MVs) | Dedup, typed timestamps (several LinkedIn date formats), derived flags (direction, own-vs-other message, has_message) |
| Gold | `connections_growth` | New connections + cumulative network size per year |
| Gold | `connections_by_company` | Network composition by employer |
| Gold | `connections_by_position` | Network composition by job title |
| Gold | `messages_by_month` | Messaging activity per month, split own vs received |
| Gold | `invitations_summary` | Invitations by direction, with personal-message share |
| Gold | `career_timeline` | Position history with current flag |
| Gold | `network_kpis` | Headline KPIs: totals, first/latest connection, top company, … |

## Data quirks handled here

* **`Connections.csv` has a 3-line preamble** before the header — ingested with `skipRows => 3`.
* CSV headers contain spaces (`First Name`, …) which Delta column names reject — every
  column is renamed to snake_case at ingestion.
* **Mixed date formats** across files: `dd MMM yyyy` (connections), `MM/d/yy, h:mm a`
  (invitations — single-digit days need `M/d`, not `MM/dd`), Java-style
  `EEE MMM dd HH:mm:ss zzz yyyy` (company follows — Spark rejects the day-of-week letter
  `E`, so it is stripped with `regexp_replace` before parsing), and
  `yyyy-MM-dd HH:mm:ss UTC` (messages).
* **Malformed rows** in `messages.csv` (unquoted multi-line content) are dropped by the
  `sent_at IS NOT NULL` filter in silver.

## Running it

1. Request your LinkedIn export ("Download your data") and place the CSVs in a Unity
   Catalog volume, e.g. `/Volumes/<catalog>/<schema>/<volume>/export`.
2. Create catalogs/schemas `personal.bronze|silver|gold` (or adjust the fully-qualified
   names in the SQL files).
3. Point the pipeline libraries at this folder and start it. All bronze tables are
   streaming tables (Auto Loader, `pathGlobFilter` per file), silver/gold are
   materialized views refreshed on every update.
