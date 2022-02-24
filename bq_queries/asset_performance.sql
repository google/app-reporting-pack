CREATE OR REPLACE TABLE {bq_project}.{bq_dataset}.asset_performance_F
AS (
WITH GeoLanguageTable AS (
    SELECT
        campaign_id,
        ARRAY_AGG(DISTINCT geo_target ORDER BY geo_target) AS geos,
        ARRAY_AGG(DISTINCT language ORDER BY language) AS languages
    FROM {bq_project}.{bq_dataset}.campaign_geo_language_target
    GROUP BY 1
),
    AppCampaignSettingsTable AS (
        SELECT
            campaign_id,
            campaign_sub_type,
            app_id,
            app_store,
            bidding_strategy,
            ARRAY_AGG(conversion_source ORDER BY conversion_source) AS conversion_sources,
            ARRAY_AGG(conversion_name ORDER BY conversion_name) AS target_conversions
        FROM {bq_project}.{bq_dataset}.app_campaign_settings
        GROUP BY 1, 2, 3, 4, 5
    )
SELECT
    PARSE_DATE("%Y-%m-%d", AP.date) AS day,
    M.account_id,
    M.account_name,
    M.currency,
    M.campaign_id,
    M.campaign_name,
    M.campaign_status,
    ACS.campaign_sub_type,
    ARRAY_TO_STRING(G.geos, " | ") AS geos,
    ARRAY_TO_STRING(G.languages, " | ") AS languages,
    ACS.app_id,
    ACS.app_store,
    ACS.bidding_strategy,
    ARRAY_TO_STRING(ACS.target_conversions, " | ")  AS target_conversions,
    "" AS firebase_bidding_status,
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
        WHEN "YOUTUBE_VIDEO" THEN "Placeholder"
        ELSE NULL
        END AS asset_orientation,
    ROUND(Videos.video_duration, 1000) AS video_duration,
		Assets.type AS asset_type,
    Assets.field_type AS field_type,
    R.performance_label AS performance_label,
    IF(R.enabled, "ENABLED", "DELETED") AS asset_status,
    AP.network AS network,
    SUM(AP.clicks) AS clicks,
    SUM(AP.impressions) AS impressions,
    ROUND(SUM(AP.cost) / 1e5, 2) AS cost,
    SUM(AP.installs) AS installs,
    SUM(AP.inapps) AS inapps,
    SUM(AP.view_through_conversions) AS view_through_conversions
FROM {bq_project}.{bq_dataset}.asset_performance AS AP
LEFT JOIN {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping AS M
  ON AP.ad_group_id = M.ad_group_id
LEFT JOIN AppCampaignSettingsTable AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN GeoLanguageTable AS G
  ON M.campaign_id =  G.campaign_id
LEFT JOIN {bq_project}.{bq_dataset}.asset_reference AS R
  ON AP.asset_id = R.asset_id
    AND AP.ad_group_id = R.ad_group_id
LEFT JOIN {bq_project}.{bq_dataset}.asset_mapping AS Assets
  ON AP.asset_id = Assets.id
LEFT JOIN {bq_project}.{bq_dataset}.videos AS Videos
  ON Assets.youtube_video_id = Videos.video_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28);
