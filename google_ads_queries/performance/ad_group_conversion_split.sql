SELECT
    segments.date AS day,
    campaign.id AS campaign_id,
    ad_group.id AS ad_group_id,
    segments.ad_network_type AS network,
    segments.conversion_action_name AS conversion_name,
    segments.conversion_action_category AS conversion_category,
    metrics.conversions AS convesions,
    metrics.all_conversions AS all_conversions
FROM ad_group
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
