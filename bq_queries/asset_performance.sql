-- Contains performance (clicks, impressions, installs, inapps, etc) on asset_id level
-- segmented by network (Search, Display, YouTube).
CREATE OR REPLACE TABLE {bq_project}.{target_dataset}.asset_performance
AS (
SELECT
    PARSE_DATE("%Y-%m-%d", AP.date) AS day,
    M.account_id,
    M.account_name,
    M.currency,
    M.campaign_id,
    M.campaign_name,
    M.campaign_status,
    ACS.campaign_sub_type,
    IFNULL(G.geos, "All") AS geos,
    IFNULL(G.languages, "All") AS languages,
    ACS.app_id,
    ACS.app_store,
    ACS.bidding_strategy,
    ACS.target_conversions,
    "" AS firebase_bidding_status,  --TODO
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    AP.asset_id,
    CASE Assets.type
        WHEN "TEXT" THEN Assets.text
        WHEN "IMAGE" THEN Assets.asset_name
        WHEN "MEDIA_BUNDLE" THEN Assets.asset_name
        WHEN "YOUTUBE_VIDEO" THEN Assets.youtube_video_title
        ELSE NULL
        END AS asset,
    CASE Assets.type
        WHEN "TEXT" THEN ""
        WHEN "IMAGE" THEN Assets.url
        WHEN "MEDIA_BUNDLE" THEN Assets.url
        WHEN "YOUTUBE_VIDEO" THEN CONCAT("https://www.youtube.com/watch?v=", Assets.youtube_video_id)
        ELSE NULL
        END AS asset_link,
    CASE Assets.type
        WHEN "TEXT" THEN ""
        WHEN "IMAGE" THEN CONCAT(Assets.height, "x", Assets.width)
        WHEN "MEDIA_BUNDLE" THEN CONCAT(Assets.height, "x", Assets.width)
        WHEN "YOUTUBE_VIDEO" THEN "Placeholder" --TODO
        ELSE NULL
        END AS asset_orientation,
    ROUND(MediaFile.video_duration / 1000) AS video_duration,
    Assets.type AS asset_type,
    Assets.field_type AS field_type,
    R.performance_label AS performance_label,
    IF(R.enabled, "ENABLED", "DELETED") AS asset_status,
    AP.network AS network,
    SUM(AP.clicks) AS clicks,
    SUM(AP.impressions) AS impressions,
    `{bq_project}.{target_dataset}.NormalizeMillis`(SUM(AP.cost)) AS cost,
    SUM(AP.installs) AS installs,
    SUM(AP.inapps) AS inapps,
    SUM(AP.view_through_conversions) AS view_through_conversions,
    SUM(AP.conversions_value) AS conversions_value
FROM {bq_project}.{bq_dataset}.asset_performance AS AP
LEFT JOIN {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping AS M
  ON AP.ad_group_id = M.ad_group_id
LEFT JOIN `{bq_project}.{target_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_project}.{target_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
LEFT JOIN {bq_project}.{bq_dataset}.asset_reference AS R
  ON AP.asset_id = R.asset_id
    AND AP.ad_group_id = R.ad_group_id
LEFT JOIN {bq_project}.{bq_dataset}.asset_mapping AS Assets
  ON AP.asset_id = Assets.id
LEFT JOIN {bq_project}.{bq_dataset}.mediafile AS MediaFile
  ON Assets.youtube_video_id = MediaFile.video_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28);
