-- Contains dynamics by asset approval status.
CREATE OR REPLACE TABLE {bq_dataset}.asset_approval_statuses_{date_iso}
AS (
SELECT
    CURRENT_DATE() AS day,
    A.ad_group_id,
    A.asset_id,
    A.field_type,
    A.approval_status,
    A.review_status,
    A.policy_topic_type,
    A.policy_topics,
    "" AS evidences
FROM {bq_dataset}.asset_reference AS A
);
