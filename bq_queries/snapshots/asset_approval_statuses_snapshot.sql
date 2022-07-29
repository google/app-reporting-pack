-- Contains dynamics by asset approval status.
CREATE OR REPLACE TABLE {bq_project}.{bq_dataset}.asset_approval_statuses_{date_iso}
AS (
SELECT
    CURRENT_DATE() AS day,
    A.*
FROM {bq_project}.{bq_dataset}.assets_disapprovals AS A
);
