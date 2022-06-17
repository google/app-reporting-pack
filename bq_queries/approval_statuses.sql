-- Contains dynamics by approval status.
CREATE OR REPLACE TABLE {bq_project}.{target_dataset}.approval_statuses
AS (
WITH
    AdGroupDisapprovals AS (
        SELECT
            day,
            ad_group_id,
            -1 AS asset_id,
            approval_status,
            review_status,
            policy_topics,
            "" AS policy_summary
        FROM `{bq_project}.{target_dataset}.ad_group_approval_statuses_*`
    ),
    AssetDisapprovals AS (
        SELECT
            day,
            ad_group_id,
            asset_id,
            approval_status,
            review_status,
            policy_topics,
            policy_summary
        FROM `{bq_project}.{target_dataset}.asset_approval_statuses_*`
    ),
    CombinedDisapprovals AS (
        SELECT
            day,
            ad_group_id,
            asset_id,
            approval_status,
            review_status,
            policy_topics,
            policy_summary
        FROM AdGroupDisapprovals
        UNION ALL
        SELECT
            day,
            ad_group_id,
            asset_id,
            approval_status,
            review_status,
            policy_topics,
            policy_summary
        FROM AssetDisapprovals
    )
SELECT
    day,
    M.account_id,
    M.account_name,
    M.currency,
    M.campaign_id,
    M.campaign_name,
    M.campaign_status,
    ACS.campaign_sub_type,
    ACS.app_id,
    ACS.app_store,
    ACS.bidding_strategy,
    ACS.start_date AS start_date,
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    IF(Disapprovals.asset_id = -1, "AdGroup", "Asset") AS disapproval_level,
    Disapprovals.asset_id,
    Disapprovals.approval_status,
    Disapprovals.review_status,
    Disapprovals.policy_topics,
    Disapprovals.policy_summary
FROM CombinedDisapprovals AS Disapprovals 
LEFT JOIN {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping AS M
  ON Disapprovals.ad_group_id = M.ad_group_id
LEFT JOIN `{bq_project}.{target_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21);
