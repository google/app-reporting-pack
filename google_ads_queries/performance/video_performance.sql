# Get meta information on each video (title & duration)
SELECT
    campaign.advertising_channel_type AS campaiggn_type,
    ad_group.id AS ad_group_id,
    video.id AS video_id,
    segments.ad_network_type AS network,
    metrics.video_views AS video_views
FROM video
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
