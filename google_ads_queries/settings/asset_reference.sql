-- Get state information on each asset - status, performance labels, policy_summary
SELECT
    ad_group.id AS ad_group_id,
    asset.id AS asset_id,
    ad_group_ad_asset_view.field_type AS field_type,
    ad_group_ad_asset_view.enabled AS enabled,
    ad_group_ad_asset_view.performance_label AS performance_label,
    ad_group_ad_asset_view.policy_summary AS policy_summary,
    asset.policy_summary.approval_status AS approval_status,
    asset.policy_summary.policy_topic_entries AS policy_topics,
    asset.policy_summary.review_status AS review_status
FROM ad_group_ad_asset_view
