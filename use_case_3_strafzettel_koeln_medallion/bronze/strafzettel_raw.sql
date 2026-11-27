-- BRONZE LAYER: raw ingestion of Cologne traffic-ticket (Strafzettel) open data via Auto Loader.
-- NOTE: part of pipeline 49d54b75-2a8a-4ef7-9f08-136c0e702478 (Strafzettel Koeln Medallion Pipeline).
-- Source: Unity Catalog volume /Volumes/opendata/stadtkoeln/stadtkoeln
-- The CSV is semicolon-delimited and ISO-8859-1 encoded; columns are kept as raw strings.
-- Any new *.csv file dropped into the volume is picked up incrementally on the next update.
CREATE OR REFRESH STREAMING TABLE opendata.bronze.strafzettel_raw
COMMENT 'Raw Köln traffic tickets 2015, one row per CSV record as ingested (all-string columns) plus file metadata'
AS SELECT *, _metadata
FROM STREAM read_files(
  '/Volumes/opendata/stadtkoeln/stadtkoeln',
  format => 'csv',
  header => 'true',
  sep => ';',
  encoding => 'ISO-8859-1',
  inferColumnTypes => false,
  pathGlobFilter => '*.csv'
)