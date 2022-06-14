CREATE OR REPLACE TABLE {bq_project}.{bq_dataset}.asset_structure_snapshot_{date}
AS (
SELECT
    CURRENT_DATE() AS date,
    A.*
FROM {bq_project}.{bq_dataset}.asset_structure AS A
);
