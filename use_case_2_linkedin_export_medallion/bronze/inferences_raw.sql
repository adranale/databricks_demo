-- BRONZE: LinkedIn inferences about the account holder
CREATE OR REFRESH STREAMING TABLE personal.bronze.inferences_raw
COMMENT 'Raw inferences-about-you export, one row per inferred attribute (all-string columns) plus file metadata'
AS SELECT
  `Category` AS category,
  `Type of inference` AS inference_type,
  `Description` AS description,
  `Inference` AS inference,
  _metadata
FROM STREAM read_files(
  '/Volumes/personal/socialmedia/linkedin/export_25112026',
  format => 'csv',
  header => 'true',
  inferColumnTypes => false,
  multiLine => true,
  pathGlobFilter => 'Inferences_about_you.csv'
)