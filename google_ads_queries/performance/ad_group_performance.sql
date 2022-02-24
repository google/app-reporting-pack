SELECT
    segments.date AS date,
    segments.ad_network_type AS network,
    ad_group.id AS ad_group_id,
    metrics.impressions AS impressions,
    metrics.clicks AS clicks,
    metrics.impressions AS impressions,
    metrics.cost_micros AS cost,
    metrics.engagements AS engagements,
    metrics.view_through_conversions AS view_through_conversions,
    metrics.video_views AS video_views,
    metrics.conversions_value AS conversions_value
FROM ad_group
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
