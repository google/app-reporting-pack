SELECT
    ad_group.id AS ad_group_id,
    ad_group_ad.action_items AS action_items
FROM ad_group_ad
WHERE campaign.advertising_channel_type = "MULTI_CHANNEL"
