-- Copyright 2023 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Contains conversion metrics (*conversions and *converions_value) on asset_id level
-- segmented by network (Search, Display, YouTube) and conversion_name.
{% if incremental == "true" %}
    CREATE OR REPLACE TABLE `{target_dataset}.asset_conversion_split_{date_iso}`
{% else %}
    CREATE OR REPLACE TABLE `{target_dataset}.asset_conversion_split`
{% endif %}
AS (
WITH CampaignCostTable AS (
    SELECT
        AP.date,
        M.campaign_id,
        `{bq_dataset}.NormalizeMillis`(SUM(AP.cost)) AS campaign_cost,
    FROM `{bq_dataset}.ad_group_performance` AS AP
    LEFT JOIN `{bq_dataset}.account_campaign_ad_group_mapping` AS M
      ON AP.ad_group_id = M.ad_group_id
    GROUP BY 1, 2
    ),
    VideoOrientation AS (
        SELECT
            video_id,
            ANY_VALUE(video_orientation) AS video_orientation
        FROM `{bq_dataset}.video_orientation`
        GROUP BY 1
    ),
    MappingTable AS (
        SELECT
            ad_group_id,
            ANY_VALUE(ad_group_name) AS ad_group_name,
            ANY_VALUE(ad_group_status) AS ad_group_status,
            ANY_VALUE(campaign_id) AS campaign_id,
            ANY_VALUE(campaign_name) AS campaign_name,
            ANY_VALUE(campaign_status) AS campaign_status,
            ANY_VALUE(account_id) AS account_id,
            ANY_VALUE(account_name) AS account_name,
            ANY_VALUE(ocid) AS ocid,
            ANY_VALUE(currency) AS currency
        FROM `{bq_dataset}.account_campaign_ad_group_mapping`
        LEFT JOIN `{bq_dataset}.ocid_mapping` USING(account_id)
        GROUP BY 1
    ),
    VideoDurations AS (
        SELECT
            video_id,
            ANY_VALUE(video_duration) AS video_duration
        FROM `{bq_dataset}.mediafile`
        WHERE video_id != ""
        GROUP BY 1
    )
SELECT
    PARSE_DATE("%Y-%m-%d", AP.date) AS day,
    M.account_id,
    M.account_name,
    M.ocid,
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
        WHEN "IMAGE" THEN Assets.url
        WHEN "YOUTUBE_VIDEO" THEN CONCAT("https://img.youtube.com/vi/", Assets.youtube_video_id, "/hqdefault.jpg")
        ELSE NULL
        END AS asset_preview_link,
    CASE Assets.type
        WHEN "TEXT" THEN ""
        WHEN "IMAGE" THEN CONCAT(Assets.height, "x", Assets.width)
        WHEN "MEDIA_BUNDLE" THEN CONCAT(Assets.height, "x", Assets.width)
        WHEN "YOUTUBE_VIDEO" THEN VideoOrientation.video_orientation
        ELSE NULL
        END AS asset_orientation,
    ROUND(VideoDurations.video_duration / 1000) AS video_duration,
    0 AS video_aspect_ratio,
    Assets.type AS asset_type,
    `{bq_dataset}.ConvertAssetFieldType`(AP.field_type) AS field_type,
    R.performance_label AS performance_label,
    IF(R.enabled, "ENABLED", "DELETED") AS asset_status,
    CASE Assets.type
        WHEN "TEXT" THEN `{bq_dataset}.BinText`(AP.field_type, LENGTH(Assets.text))
        WHEN "IMAGE" THEN `{bq_dataset}.BinBanners`(Assets.height, Assets.width)
        WHEN "MEDIA_BUNDLE" THEN `{bq_dataset}.BinBanners`(Assets.height, Assets.width)
        WHEN "YOUTUBE_VIDEO" THEN VideoOrientation.video_orientation
        END AS asset_dimensions,
    `{bq_dataset}.ConvertAdNetwork`(AP.network) AS network,
    ConversionMapping.conversion_type AS conversion_category,
    ConversionMapping.conversion_name,
    SUM(all_conversions) AS all_conversions,
    SUM(conversions_value) AS conversions_value,
    SUM(all_conversions_value) AS all_conversions_value,
    SUM(view_through_conversions) AS view_through_conversions
FROM `{bq_dataset}.asset_conversion_split` AS AP
LEFT JOIN `{bq_dataset}.app_conversions_mapping` AS ConversionMapping
  ON AP.conversion_id = ConversionMapping.conversion_id
LEFT JOIN MappingTable AS M
  ON AP.ad_group_id = M.ad_group_id
LEFT JOIN CampaignCostTable AS CampCost
    ON AP.date = CampCost.date
      AND M.campaign_id = CampCost.campaign_id
LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
LEFT JOIN `{bq_dataset}.asset_reference` AS R
  ON AP.asset_id = R.asset_id
    AND AP.ad_group_id = R.ad_group_id
    AND AP.field_type = R.field_type
LEFT JOIN `{bq_dataset}.asset_mapping` AS Assets
  ON AP.asset_id = Assets.id
LEFT JOIN VideoDurations
  ON Assets.youtube_video_id = VideoDurations.video_id
LEFT JOIN VideoOrientation
    ON Assets.youtube_video_id = VideoOrientation.video_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
);
