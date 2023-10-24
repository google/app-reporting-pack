-- Copyright 2022 Google LLC
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
    WITH ConversionsTable AS (
        SELECT
            ConvSplit.date,
            ConvSplit.network,
            ConvSplit.ad_group_id,
            SUM(ConvSplit.conversions) AS conversions,
            SUM(IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
            SUM(
                IF(LagAdjustments.lag_adjustment IS NULL,
                    IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0),
                    ROUND(IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0) / LagAdjustments.lag_adjustment))
            ) AS installs_adjusted,
            SUM(IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0)) AS inapps,
            SUM(
                IF(LagAdjustments.lag_adjustment IS NULL,
                    IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0),
                    ROUND(IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0) / LagAdjustments.lag_adjustment))
            ) AS inapps_adjusted
        FROM {bq_dataset}.ad_group_conversion_split AS ConvSplit
        LEFT JOIN `{bq_dataset}.ConversionLagAdjustments` AS LagAdjustments
            ON PARSE_DATE("%Y-%m-%d", ConvSplit.date) = LagAdjustments.adjustment_date
                AND ConvSplit.network = LagAdjustments.network
                AND ConvSplit.conversion_id = LagAdjustments.conversion_id
        GROUP BY 1, 2, 3
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
    "" AS firebase_bidding_status,
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    `{bq_dataset}.ConvertAdNetwork`(AP.network) AS network,
    SUM(AP.clicks) AS clicks,
    SUM(AP.impressions) AS impressions,
    `{bq_dataset}.NormalizeMillis`(SUM(AP.cost)) AS cost,
    SUM(conversions) AS conversions,
    SUM(installs) AS installs,
    SUM(installs_adjusted) AS installs_adjusted,
    SUM(inapps) AS inapps,
    SUM(inapps_adjusted) AS inapps_adjusted,
    SUM(AP.view_through_conversions) AS view_through_conversions,
    SUM(AP.video_views) AS video_views,
    SUM(AP.conversions_value) AS conversions_value
FROM {bq_dataset}.ad_group_performance AS AP
LEFT JOIN ConversionsTable AS ConvSplit
    USING(date, ad_group_id, network)
LEFT JOIN MappingTable AS M
  ON AP.ad_group_id = M.ad_group_id
LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
