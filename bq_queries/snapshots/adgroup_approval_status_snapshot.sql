-- Contains dynamics by ad_group approval status.
CREATE OR REPLACE TABLE {bq_dataset}.ad_group_approval_statuses_{date_iso}
AS (
SELECT
    CURRENT_DATE() AS day,
    A.ad_group_id,
    A.approval_status,
    A.review_status,
    A.policy_topic_type,
    A.policy_topics,
    "" AS evidences
FROM {bq_dataset}.ad_group_ad_disapprovals AS A
);
