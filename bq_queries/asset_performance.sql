-- Contains performance (clicks, impressions, installs, inapps, etc) on asset_id level
-- segmented by network (Search, Display, YouTube).
CREATE TEMP FUNCTION GetCohort(arr ARRAY<FLOAT64>, day INT64)
RETURNS FLOAT64
AS (
    arr[SAFE_OFFSET(day)]
);

CREATE OR REPLACE TABLE {target_dataset}.asset_performance
AS (
WITH CampaignCostTable AS (
    SELECT
        AP.date,
        M.campaign_id,
        `{bq_dataset}.NormalizeMillis`(SUM(AP.cost)) AS campaign_cost,
    FROM {bq_dataset}.ad_group_performance AS AP
    LEFT JOIN {bq_dataset}.account_campaign_ad_group_mapping AS M
      ON AP.ad_group_id = M.ad_group_id
    GROUP BY 1, 2
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
        WHEN "TEXT" THEN Assets.text WHEN "IMAGE" THEN Assets.asset_name
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
    0 AS video_aspect_ratio,
    Assets.type AS asset_type,
    AP.field_type AS field_type,
    R.performance_label AS performance_label,
    IF(R.enabled, "ENABLED", "DELETED") AS asset_status,
    AP.network AS network,
    SUM(AP.clicks) AS clicks,
    SUM(AP.impressions) AS impressions,
    `{bq_dataset}.NormalizeMillis`(SUM(AP.cost)) AS cost,
    ANY_VALUE(CampCost.campaign_cost) AS campaign_cost,
    -- TODO: provide correct enums
    SUM(IF(bidding_strategy = "", 0, `{bq_dataset}.NormalizeMillis`(AP.cost))) AS cost_non_install_campaigns,
    SUM(IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
            AP.installs, AP.inapps)) AS conversions,
    SUM(AP.installs) AS installs,
    SUM(
        IF(LagAdjustmentsInstalls.lag_adjustment IS NULL,
            AP.installs,
            ROUND(AP.installs / LagAdjustmentsInstalls.lag_adjustment))
    ) AS installs_adjusted,
    SUM(AP.inapps) AS inapps,
    SUM(
        IF(LagAdjustmentsInapps.lag_adjustment IS NULL,
            AP.inapps,
            ROUND(AP.inapps / LagAdjustmentsInapps.lag_adjustment))
    ) AS inapps_adjusted,
    SUM(AP.view_through_conversions) AS view_through_conversions,
    SUM(AP.conversions_value) AS conversions_value,
    {% for day in cohort_days %}
        SUM(GetCohort(AssetCohorts.lag_data.installs, {{day}})) AS installs_{{day}}_day,
        SUM(GetCohort(AssetCohorts.lag_data.inapps, {{day}})) AS inapps_{{day}}_day,
        SUM(GetCohort(AssetCohorts.lag_data.conversions_value, {{day}})) AS conversions_value_{{day}}_day,
    {% endfor %}
FROM {bq_dataset}.asset_performance AS AP
LEFT JOIN {bq_dataset}.account_campaign_ad_group_mapping AS M
  ON AP.ad_group_id = M.ad_group_id
LEFT JOIN CampaignCostTable AS CampCost
    ON AP.date = CampCost.date
      AND M.campaign_id = CampCost.campaign_id
LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
LEFT JOIN `{bq_dataset}.ConversionLagAdjustments` AS LagAdjustmentsInstalls
    ON PARSE_DATE("%Y-%m-%d", AP.date) = LagAdjustmentsInstalls.adjustment_date
        AND AP.network = LagAdjustmentsInstalls.network
        AND ACS.install_conversion_id = LagAdjustmentsInstalls.conversion_id
LEFT JOIN `{bq_dataset}.ConversionLagAdjustments` AS LagAdjustmentsInapps
    ON PARSE_DATE("%Y-%m-%d", AP.date) = LagAdjustmentsInapps.adjustment_date
        AND AP.network = LagAdjustmentsInapps.network
        AND ACS.inapp_conversion_id = LagAdjustmentsInapps.conversion_id
LEFT JOIN {bq_dataset}.asset_reference AS R
  ON AP.asset_id = R.asset_id
    AND AP.ad_group_id = R.ad_group_id
    AND AP.field_type = R.field_type
LEFT JOIN {bq_dataset}.asset_mapping AS Assets
  ON AP.asset_id = Assets.id
LEFT JOIN (SELECT video_id, video_duration FROM {bq_dataset}.mediafile WHERE video_id != "")  AS MediaFile
  ON Assets.youtube_video_id = MediaFile.video_id
LEFT JOIN `{bq_dataset}.AssetCohorts` AS AssetCohorts
    ON PARSE_DATE("%Y-%m-%d", AP.date) = AssetCohorts.day_of_interaction
        AND AP.ad_group_id = AssetCohorts.ad_group_id
        AND AP.network = AssetCohorts.network
        AND AP.asset_id = AssetCohorts.asset_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29);
