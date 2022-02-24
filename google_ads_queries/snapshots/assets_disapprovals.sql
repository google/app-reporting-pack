SELECT
    asset.id AS asset_id,
    asset.policy_summary.approval_status AS approval_status,
    asset.policy_summary.policy_topic_entries AS policy_topics,
    asset.policy_summary.review_status AS review_status,
    ad_group_ad_asset_view.policy_summary AS policy_summary
FROM ad_group_ad_asset_view
WHERE asset.policy_summary.approval_status != "APPROVED"
