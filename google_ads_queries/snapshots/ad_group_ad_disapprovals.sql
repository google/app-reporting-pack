SELECT
    ad_group.id AS ad_group_id,
    ad_group_ad.policy_summary.approval_status AS approval_status,
    ad_group_ad.policy_summary.review_status AS review_status,
    ad_group_ad.policy_summary.policy_topic_entries AS policy_topics 
FROM ad_group_ad
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND campaign.status = "ENABLED"
