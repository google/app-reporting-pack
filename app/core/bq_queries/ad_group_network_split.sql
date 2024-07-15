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

-- Contains ad group level performance segmented by network (Search, Display, YouTube).
{% if incremental == "true" %}
CREATE OR REPLACE TABLE `{target_dataset}.ad_group_network_split_{date_iso}`
{% else %}
CREATE OR REPLACE TABLE `{target_dataset}.ad_group_network_split`
{% endif %}
AS (
  WITH
    ConversionsTable AS (
      SELECT
        CS.date,
        CS.network,
        CS.ad_group_id,
        SUM(CS.conversions) AS conversions,
        SUM(IF(CS.conversion_category = 'DOWNLOAD', conversions, 0))
          AS installs,
        SUM(
          IF(
            LA.lag_adjustment IS NULL,
            IF(CS.conversion_category = 'DOWNLOAD', conversions, 0),
            ROUND(
              IF(CS.conversion_category = 'DOWNLOAD', conversions, 0)
              / LA.lag_adjustment))) AS installs_adjusted,
        SUM(IF(CS.conversion_category != 'DOWNLOAD', conversions, 0))
          AS inapps,
        SUM(
          IF(
            LA.lag_adjustment IS NULL,
            IF(CS.conversion_category != 'DOWNLOAD', conversions, 0),
            ROUND(
              IF(CS.conversion_category != 'DOWNLOAD', conversions, 0)
              / LA.lag_adjustment))) AS inapps_adjusted
      FROM `{bq_dataset}.ad_group_conversion_split` AS CS
      LEFT JOIN `{bq_dataset}.ConversionLagAdjustments` AS LA
        ON
          PARSE_DATE('%Y-%m-%d', CS.date)
            = LA.adjustment_date
          AND CS.network = LA.network
          AND CS.conversion_id = LA.conversion_id
      GROUP BY 1, 2, 3
    ),
    MappingTable AS (
      SELECT
        M.ad_group_id,
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
    CustomConvSplit AS (
      SELECT
        date,
        ad_group_id,
        network,
        {% for custom_conversion in custom_conversions %}
        {% for conversion_alias, conversion_name in custom_conversion.items() %}
        SUM(IF(conversion_name IN ('{{conversion_name}}'), all_conversions, 0))
          AS conversions_{{conversion_alias}},
        SUM(
          IF(
            conversion_name IN ('{{conversion_name}}'),
            all_conversions_value,
            0)) AS conversions_value_{{conversion_alias}},
      {% endfor %}
      {% endfor %}
      FROM `{bq_dataset}.ad_group_conversion_split`
      GROUP BY 1, 2, 3
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
    `{bq_dataset}.ConvertAdNetwork`(AP.network) AS network,
    SUM(AP.clicks) AS clicks,
    SUM(AP.impressions) AS impressions,
    `{bq_dataset}.NormalizeMillis`(SUM(AP.cost)) AS cost,
    SUM(CS.conversions) AS conversions,
    SUM(CS.installs) AS installs,
    SUM(CS.installs_adjusted) AS installs_adjusted,
    SUM(CS.inapps) AS inapps,
    SUM(CS.inapps_adjusted) AS inapps_adjusted,
    SUM(AP.view_through_conversions) AS view_through_conversions,
    SUM(AP.video_views) AS video_views,
    SUM(AP.conversions_value) AS conversions_value,
    {% for custom_conversion in custom_conversions %}
    {% for conversion_alias, conversion_name in custom_conversion.items() %}
    SUM(COALESCE(CCS.conversions_{{conversion_alias}}, 0))
      AS conversions_{{conversion_alias}},
    SUM(COALESCE(CCS.conversions_value_{{conversion_alias}}, 0))
      AS conversions_value_{{conversion_alias}},
  {% endfor %}
  {% endfor %}
  FROM {bq_dataset}.ad_group_performance AS AP
  LEFT JOIN ConversionsTable AS CS
    USING (date, ad_group_id, network)
  LEFT JOIN MappingTable AS M
    ON AP.ad_group_id = M.ad_group_id
  LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
    ON M.campaign_id = ACS.campaign_id
  LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
    ON M.campaign_id = G.campaign_id
  LEFT JOIN CustomConvSplit AS CCS
    ON
      AP.date = CCS.date
      AND AP.ad_group_id = CCS.ad_group_id
      AND AP.network = CCS.network
  GROUP BY ALL
);
