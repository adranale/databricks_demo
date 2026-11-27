# Use Case 3 — Strafzettel Köln 2015 (Parking-Ticket Open Data Analytics)

A **Lakeflow Spark Declarative Pipeline** that models the City of Cologne's published 2015
parking-ticket ("Strafzettel") data through the Medallion Architecture in Unity Catalog
(catalog: `opendata`).

## Source data

The raw CSV is open data and available online on Kaggle:
**[Koeln Strafzettel 2015](https://www.kaggle.com/datasets/adranale/koeln-strafzettel-2015)**

The original open-data portal of the City of Cologne publishes the annual ticket extracts;
this dataset is the 2015 export (~970k tickets; semicolon-delimited, ISO-8859-1 encoding,
one row per issued ticket).

## Pipeline layout

| Layer | Dataset | Description |
| :--- | :--- | :--- |
| Bronze | `strafzettel_raw` | Auto Loader streaming table on the raw CSV (separator `;`, ISO-8859-1, no type inference) |
| Silver | `strafzettel_cleaned` | Deduplicated (exact-duplicate tickets removed), typed timestamps, plate-origin classification (Köln `K` / other German district / foreign), temporal fields (weekday, hour, weekend flag) |
| Gold | `strafzettel_by_street` | Ticket counts + shares per street |
| Gold | `strafzettel_by_time` | Day × hour matrix |
| Gold | `strafzettel_by_violation` | Counts per violation code (Tatbestandsnummer), labels for the best-documented code families |
| Gold | `strafzettel_by_vehicle_brand` | Counts by brand and vehicle type |
| Gold | `strafzettel_by_plate_origin` | Local vs. non-local vehicle share |
| Gold | `strafzettel_kpis` | Headline KPIs: totals, top street, top violation, distinct streets, local share |

## Example analyses enabled

* Enforcement hotspots: which streets accumulate the most tickets
* Violation mix: sidewalk parking and parking-time-exceeded dominate the 2015 data
* Local vs. out-of-town vehicles: how the plate origin changes with weekday
* Temporal patterns: weekday vs. weekend volumes and daytime peaks

## Running it

1. Download the dataset from Kaggle (link above) and place the CSV in a Unity Catalog
   volume (the pipeline reads it with Auto Loader, so incremental file drops are picked up
   automatically).
2. Create the `opendata.bronze|silver|gold` schemas (or adjust the names in the SQL files).
3. Point the pipeline libraries at this folder and start it.

## Attribution

Data source: City of Cologne open-data portal (Strafzettel / Verwarnungen 2015), republished
on Kaggle. Contains public sector information made available under the open-data licence of
the City of Cologne.
