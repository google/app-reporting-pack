SELECT
    segments.date,
    ad_group.id,
    user_location_view.country_criterion_id,
    metrics.impressions,
    metrics.clicks,
    metrics.cost_micros
FROM user_location_view
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
