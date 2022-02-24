SELECT
    segments.date AS date,
    ad_group.id AS ad_group_id,
    detail_placement_view.display_name AS placement_name,
    segments.ad_network_type AS network,
    metrics.clicks AS clicks,
    metrics.impressions AS impressions,
    metrics.cost_micros AS cost,
    metrics.conversions AS conversions
FROM detail_placement_view
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
