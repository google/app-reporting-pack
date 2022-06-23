SELECT
    segments.date AS date,
    segments.ad_network_type AS network,
    campaign.advertising_channel_type AS campaign_type,
    ad_group.id AS ad_group_id,
    user_location_view.country_criterion_id AS country_id,
    segments.conversion_action_category AS conversion_category,
    metrics.conversions AS conversions
FROM user_location_view
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
