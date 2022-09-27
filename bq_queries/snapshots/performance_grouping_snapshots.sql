-- Contains dynamics by asset performance grouping status.
CREATE OR REPLACE TABLE {bq_dataset}.performance_grouping_statuses_{date_iso}
AS (
SELECT
    CURRENT_DATE() AS day,
    A.ad_group_id,
    A.asset_id,
    A.performance_label
FROM {bq_dataset}.asset_reference AS A
);
