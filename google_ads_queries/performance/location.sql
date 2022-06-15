SELECT
    segments.date AS date,
    segments.ad_network_type AS network,
    campaign.advertising_channel_type AS campaign_type,
    ad_group.id AS ad_group_id,
    user_location_view.country_criterion_id AS country_id,
    metrics.impressions AS impressions,
    metrics.clicks AS clicks,
    metrics.cost_micros AS cost,
    metrics.video_views AS video_views,
    metrics.interactions AS interactions
FROM user_location_view
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
