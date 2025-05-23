-- Copyright 2024 Google LLC
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

-- Contains performance (clicks, impressions, installs, inapps, etc) on asset_id level
-- segmented by network (Search, Display, YouTube).
CREATE TEMP FUNCTION GetCohort(arr ARRAY<FLOAT64>, day INT64)
RETURNS FLOAT64
AS (
  arr[SAFE_OFFSET(day)]
);

{% if incremental == "true" %}
CREATE OR REPLACE TABLE `{target_dataset}.asset_performance_{date_iso}`
{% else %}
CREATE OR REPLACE TABLE `{target_dataset}.asset_performance`
{% endif %}
AS (
  WITH
    CampaignCostTable AS (
      SELECT
        AP.date,
        M.campaign_id,
        `{bq_dataset}.NormalizeMillis`(SUM(AP.cost)) AS campaign_cost,
      FROM `{bq_dataset}.ad_group_performance` AS AP
      LEFT JOIN
        (
          SELECT ad_group_id, ANY_VALUE(campaign_id) AS campaign_id
          FROM `{bq_dataset}.account_campaign_ad_group_mapping`
          GROUP BY 1
        ) AS M
        ON AP.ad_group_id = M.ad_group_id
      GROUP BY 1, 2
    ),
    ConversionCategoryMapping AS (
      SELECT
        conversion_id,
        ANY_VALUE(conversion_type) AS conversion_category
      FROM `{bq_dataset}.app_conversions_mapping`
      GROUP BY 1
    ),
    AssetsConversionsAdjustedTable AS (
      SELECT
        CS.date,
        CS.network,
        CS.ad_group_id,
        CS.asset_id,
        CS.field_type,
        SUM(CS.conversions) AS conversions,
        SUM(
          IF(
            LA.lag_adjustment IS NULL,
            IF(M.conversion_category = 'DOWNLOAD', conversions, 0),
            ROUND(
              IF(M.conversion_category = 'DOWNLOAD', conversions, 0)
              / LA.lag_adjustment))) AS installs_adjusted,
        SUM(
          IF(
            LA.lag_adjustment IS NULL,
            IF(M.conversion_category != 'DOWNLOAD', conversions, 0),
            ROUND(
              IF(M.conversion_category != 'DOWNLOAD', conversions, 0)
              / LA.lag_adjustment))) AS inapps_adjusted
      FROM `{bq_dataset}.asset_conversion_split` AS CS
      LEFT JOIN ConversionCategoryMapping AS M
        ON CS.conversion_id = M.conversion_id
      LEFT JOIN `{bq_dataset}.ConversionLagAdjustments` AS LA
        ON PARSE_DATE('%Y-%m-%d', CS.date) = LA.adjustment_date
          AND CS.network = LA.network
          AND CS.conversion_id = LA.conversion_id
      GROUP BY 1, 2, 3, 4, 5
    ),
    VideoDurations AS (
      SELECT
        video_id,
        ANY_VALUE(video_duration) AS video_duration
      FROM `{bq_dataset}.mediafile`
      WHERE video_id != ''
      GROUP BY 1
    ),
    MappingTable AS (
      SELECT
        ad_group_id,
        ANY_VALUE(M.ad_group_name) AS ad_group_name,
        ANY_VALUE(M.ad_group_status) AS ad_group_status,
        ANY_VALUE(M.campaign_id) AS campaign_id,
        ANY_VALUE(M.campaign_name) AS campaign_name,
        ANY_VALUE(M.campaign_status) AS campaign_status,
        ANY_VALUE(M.account_id) AS account_id,
        ANY_VALUE(M.account_name) AS account_name,
        ANY_VALUE(O.ocid) AS ocid,
        ANY_VALUE(M.currency) AS currency
      FROM `{bq_dataset}.account_campaign_ad_group_mapping` AS M
      LEFT JOIN `{bq_dataset}.ocid_mapping` AS O
        USING (account_id)
      GROUP BY 1
    ),
    AssetReferenceTable AS (
      SELECT
        asset_id,
        field_type,
        ANY_VALUE(ad_group_id) AS ad_group_id,
        ANY_VALUE(performance_label) AS performance_label,
        ANY_VALUE(enabled) AS enabled
      FROM `{bq_dataset}.asset_reference`
      GROUP BY 1, 2
    ),
    AssetMapping AS (
      SELECT
        id,
        ANY_VALUE(type) AS type,
        ANY_VALUE(text) AS text,
        ANY_VALUE(asset_name) AS asset_name,
        ANY_VALUE(youtube_video_title) AS youtube_video_title,
        ANY_VALUE(url) AS url,
        ANY_VALUE(youtube_video_id) AS youtube_video_id,
        ANY_VALUE(height) AS height,
        ANY_VALUE(width) AS width
      FROM `{bq_dataset}.asset_mapping`
      GROUP BY 1
    ),
    VideoOrientation AS (
      SELECT
        video_id,
        ANY_VALUE(video_orientation) AS video_orientation
      FROM `{bq_dataset}.video_orientation`
      GROUP BY 1
    ),
    ConversionMapping AS (
      SELECT DISTINCT
        conversion_id,
        conversion_name
      FROM `{bq_dataset}.app_conversions_mapping`
    ),
    CustomConvSplit AS (
      SELECT
        C.date,
        C.ad_group_id,
        C.asset_id,
        C.field_type,
        C.network,
        {% for custom_conversion in custom_conversions %}
        {% for conversion_alias, conversion_name in custom_conversion.items() %}
        SUM(
          IF(
            CM.conversion_name IN ('{{conversion_name}}'),
            C.all_conversions,
            0)) AS conversions_{{conversion_alias}},
        SUM(
          IF(
            CM.conversion_name IN ('{{conversion_name}}'),
            C.all_conversions_value,
            0)) AS conversions_value_{{conversion_alias}},
      {% endfor %}
      {% endfor %}
      FROM `{bq_dataset}.asset_conversion_split` AS C
      LEFT JOIN ConversionMapping AS CM
        ON C.conversion_id = CM.conversion_id
      GROUP BY 1, 2, 3, 4, 5
    )
  SELECT
    PARSE_DATE('%Y-%m-%d', AP.date) AS day,
    M.account_id,
    M.account_name,
    M.ocid,
    M.currency,
    M.campaign_id,
    M.campaign_name,
    M.campaign_status,
    ACS.campaign_sub_type,
    IFNULL(G.geos, 'All') AS geos,
    IFNULL(G.languages, 'All') AS languages,
    ACS.app_id,
    ACS.app_store,
    ACS.bidding_strategy,
    ACS.target_conversions,
    '' AS firebase_bidding_status,
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    AP.asset_id,
    CASE A.type
      WHEN 'TEXT' THEN A.text
      WHEN 'IMAGE' THEN A.asset_name
      WHEN 'MEDIA_BUNDLE' THEN A.asset_name
      WHEN 'YOUTUBE_VIDEO' THEN A.youtube_video_title
      ELSE NULL
      END AS asset,
    CASE A.type
      WHEN 'TEXT' THEN ''
      WHEN 'IMAGE' THEN A.url
      WHEN 'MEDIA_BUNDLE' THEN A.url
      WHEN 'YOUTUBE_VIDEO'
        THEN CONCAT('https://www.youtube.com/watch?v=', A.youtube_video_id)
      ELSE NULL
      END AS asset_link,
    CASE A.type
      WHEN 'IMAGE' THEN A.url
      WHEN 'YOUTUBE_VIDEO'
        THEN
          CONCAT(
            'https://img.youtube.com/vi/',
            A.youtube_video_id,
            '/hqdefault.jpg')
      ELSE NULL
      END AS asset_preview_link,
    CASE A.type
      WHEN 'TEXT' THEN ''
      WHEN 'IMAGE' THEN CONCAT(A.height, 'x', A.width)
      WHEN 'MEDIA_BUNDLE' THEN CONCAT(A.height, 'x', A.width)
      WHEN 'YOUTUBE_VIDEO' THEN VO.video_orientation
      ELSE NULL
      END AS asset_orientation,
    video.video_views AS video_views,
    video.p25_view AS p25_view,
    video.p50_view AS p50_view,
    video.p75_view AS p75_view,
    video.p100_view AS p100_view,
    ROUND(VD.video_duration / 1000) AS video_duration,
    0 AS video_aspect_ratio,
    A.type AS asset_type,
    `{bq_dataset}.ConvertAssetFieldType`(AP.field_type) AS field_type,
    R.performance_label AS performance_label,
    IF(R.enabled, 'ENABLED', 'DELETED') AS asset_status,
    CASE A.type
      WHEN 'TEXT'
        THEN `{bq_dataset}.BinText`(AP.field_type, LENGTH(A.text))
      WHEN 'IMAGE' THEN `{bq_dataset}.BinBanners`(A.height, A.width)
      WHEN 'MEDIA_BUNDLE'
        THEN `{bq_dataset}.BinBanners`(A.height, A.width)
      WHEN 'YOUTUBE_VIDEO' THEN VO.video_orientation
      END AS asset_dimensions,
    `{bq_dataset}.ConvertAdNetwork`(AP.network) AS network,
    SUM(AP.clicks) AS clicks,
    SUM(AP.impressions) AS impressions,
    `{bq_dataset}.NormalizeMillis`(SUM(AP.cost)) AS cost,
    ANY_VALUE(CC.campaign_cost) AS campaign_cost,
    SUM(
      IF(
        ACS.bidding_strategy IN ('Installs', 'Installs Advanced'),
        0,
        `{bq_dataset}.NormalizeMillis`(AP.cost))) AS cost_non_install_campaigns,
    SUM(IF(ACS.bidding_strategy = 'Installs', AP.installs, AP.inapps))
      AS conversions,
    SUM(AP.installs) AS installs,
    SUM(CS.installs_adjusted) AS installs_adjusted,
    SUM(AP.inapps) AS inapps,
    SUM(CS.inapps_adjusted) AS inapps_adjusted,
    SUM(AP.view_through_conversions) AS view_through_conversions,
    SUM(AP.conversions_value) AS conversions_value,
    {% for custom_conversion in custom_conversions %}
    {% for conversion_alias, conversion_name in custom_conversion.items() %}
    SUM(COALESCE(CCS.conversions_{{conversion_alias}}, 0))
      AS conversions_{{conversion_alias}},
    SUM(COALESCE(CCS.conversions_value_{{conversion_alias}}, 0))
      AS conversions_value_{{conversion_alias}},
    {% endfor %}
    {% endfor %}
    {% for day in cohort_days %}
    SUM(GetCohort(AC.lag_data.installs, {{day}}))
      AS installs_{{day}}_day,
    SUM(GetCohort(AC.lag_data.inapps, {{day}})) AS inapps_{{day}}_day,
    SUM(GetCohort(AC.lag_data.conversions_value, {{day}}))
      AS conversions_value_{{day}}_day,
  {% endfor %}
  FROM `{bq_dataset}.asset_performance` AS AP
  LEFT JOIN AssetsConversionsAdjustedTable AS CS
    ON
      AP.date = CS.date
      AND Ap.ad_group_id = CS.ad_group_id
      AND AP.network = CS.network
      AND AP.asset_id = CS.asset_id
      AND AP.field_type = CS.field_type
  LEFT JOIN MappingTable AS M
    ON AP.ad_group_id = M.ad_group_id
  LEFT JOIN CampaignCostTable AS CC
    ON
      AP.date = CC.date
      AND M.campaign_id = CC.campaign_id
  LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
    ON M.campaign_id = ACS.campaign_id
  LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
    ON M.campaign_id = G.campaign_id
  LEFT JOIN AssetReferenceTable AS R
    ON
      AP.asset_id = R.asset_id
      AND AP.ad_group_id = R.ad_group_id
      AND AP.field_type = R.field_type
  LEFT JOIN AssetMapping AS A
    ON AP.asset_id = A.id
  LEFT JOIN {bq_dataset}.video AS video
    ON A.youtube_video_id = video.video_id
  LEFT JOIN VideoDurations AS VD
    ON A.youtube_video_id = VD.video_id
  LEFT JOIN VideoOrientation AS VO
    ON A.youtube_video_id = VO.video_id
  LEFT JOIN `{bq_dataset}.AssetCohorts` AS AC
    ON
      PARSE_DATE('%Y-%m-%d', AP.date) = AC.day_of_interaction
      AND AP.ad_group_id = AC.ad_group_id
      AND AP.network = AC.network
      AND AP.asset_id = AC.asset_id
      AND AP.field_type = AC.field_type
  LEFT JOIN CustomConvSplit AS CCS
    ON
      AP.date = CCS.date
      AND AP.ad_group_id = CCS.ad_group_id
      AND AP.network = CCS.network
      AND AP.asset_id = CCS.asset_id
      AND AP.field_type = CCS.field_type
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
    22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
);
