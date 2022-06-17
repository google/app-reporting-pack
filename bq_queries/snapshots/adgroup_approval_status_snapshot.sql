-- Contains dynamics by ad_group approval status.
CREATE OR REPLACE TABLE {bq_project}.{target_dataset}.ad_group_approval_statuses_{date_iso}
AS (
SELECT
    CURRENT_DATE() AS day,
    A.*
FROM {bq_project}.{bq_dataset}.ad_group_ad_disapprovals AS A
);
