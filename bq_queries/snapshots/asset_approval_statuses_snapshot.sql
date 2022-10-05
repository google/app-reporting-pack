-- Contains dynamics by asset approval status.
CREATE OR REPLACE TABLE {bq_dataset}.asset_approval_statuses_{date_iso}
AS (
SELECT
    CURRENT_DATE() AS day,
    A.ad_group_id,
    A.asset_id,
    A.field_type,
    A.approval_status,
    A.policy_topics,
    A.review_status,
    A.policy_summary
FROM {bq_dataset}.asset_reference AS A
);
