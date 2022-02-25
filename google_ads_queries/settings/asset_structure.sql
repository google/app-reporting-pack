SELECT
    ad_group.id AS ad_group_id,
    ad_group_ad.ad.type AS type,
    ad_group_ad.ad_strength AS ad_strength,
    ad_group_ad.ad.app_ad.descriptions AS install_descriptions,
    ad_group_ad.ad.app_ad.headlines AS install_headlines,
    ad_group_ad.ad.app_ad.html5_media_bundles AS install_media_bundles,
    ad_group_ad.ad.app_ad.images AS install_images,
    ad_group_ad.ad.app_ad.youtube_videos AS install_videos,
    ad_group_ad.ad.app_engagement_ad.descriptions AS engagement_descriptions,
    ad_group_ad.ad.app_engagement_ad.headlines AS engagement_headlines,
    ad_group_ad.ad.app_engagement_ad.images AS engagement_images,
    ad_group_ad.ad.app_engagement_ad.videos AS engagement_videos,
    ad_group_ad.ad.app_pre_registration_ad.descriptions AS pre_registration_descriptions,
    ad_group_ad.ad.app_pre_registration_ad.headlines AS pre_registration_headlines,
    ad_group_ad.ad.app_pre_registration_ad.images AS pre_registration_images,
    ad_group_ad.ad.app_pre_registration_ad.youtube_videos AS pre_registration_videos
FROM ad_group_ad
WHERE campaign.advertising_channel_type = "MULTI_CHANNEL"
