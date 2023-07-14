# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

-- Contains campaign level iOS SKAN performance with decoded events from SKAN input schema
CREATE OR REPLACE TABLE `{target_dataset}.skan_decoder`
AS (
  WITH MappingTable AS (
    SELECT
      campaign_id,
      ANY_VALUE(campaign_name) AS campaign_name,
      ANY_VALUE(campaign_status) AS campaign_status,
      ANY_VALUE(account_id) AS account_id,
      ANY_VALUE(account_name) AS account_name,
      ANY_VALUE(currency) AS currency
    FROM `{bq_dataset}.account_campaign_ad_group_mapping`
    GROUP BY 1
  ),
  SkanInputSchema AS (
    SELECT
      app_id,
      CAST(skan_conversion_value AS INT64) AS skan_conversion_value,
      skan_event_count,
      skan_event_value_low,
      skan_event_value_high,
      skan_event_value_mean,
      skan_mapped_event
    FROM `{bq_dataset}.skan_schema`
  ),
  PreparedData AS (
    SELECT
      PARSE_DATE("%Y-%m-%d", CAST(date AS STRING)) AS day,
      M.account_id,
      M.account_name,
      M.currency,
      M.campaign_id,
      M.campaign_name,
      M.campaign_status,
      ACS.campaign_sub_type,
      ACS.app_id,
      ACS.app_store,
      ACS.bidding_strategy,
      ACS.target_conversions,
      S.skan_mapped_event,
      SP.skan_conversion_value,
      SP.skan_source_app_id,
      SP.skan_user_type,
      SP.skan_ad_event_type,
      SP.skan_ad_network_attribution_credit,
      SUM(IF(SP.skan_conversion_value IS NULL, SP.skan_postbacks, 0)) AS sum_null_values,
      SUM(IF(CAST(SP.skan_conversion_value AS INT64) = 0, SP.skan_postbacks, 0)) AS sum_zero_values,
      SUM(IF(SP.skan_conversion_value IS NOT NULL OR CAST(SP.skan_conversion_value AS INT64) != 0, SP.skan_postbacks, 0)) AS sum_wo_null_zero_values,
      SUM(SP.skan_postbacks) AS skan_postbacks,
      SUM(IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST", SP.skan_postbacks, 0)) AS tcpi_skan_postbacks,
      SUM(IF(ACS.bidding_strategy = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST", SP.skan_postbacks, 0)) AS tcpi_advanced_skan_postbacks,
      SUM(IF(ACS.bidding_strategy IN ("OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST","OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST"), SP.skan_postbacks, 0)) AS tcpa_skan_postbacks,
      SUM(IF(ACS.bidding_strategy = "OPTIMIZE_RETURN_ON_ADVERTISING_SPEND", SP.skan_postbacks, 0)) AS troas_skan_postbacks,
      SUM(S.skan_event_count) AS skan_event_count,
      SUM(S.skan_event_value_low) AS skan_event_value_low,
      SUM(S.skan_event_value_high) AS skan_event_value_high,
      SUM(S.skan_event_value_mean) AS skan_event_value_mean
    FROM `{bq_dataset}.ios_campaign_skan_performance` AS SP
    LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
      USING(campaign_id)
    LEFT JOIN SkanInputSchema AS S
      ON CAST(SP.skan_conversion_value AS INT64) = S.skan_conversion_value
        AND ACS.app_id = S.app_id
    LEFT JOIN MappingTable AS M
      USING(campaign_id)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
  )
  SELECT
    day,
    account_id,
    account_name,
    currency,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_sub_type,
    app_id,
    app_store,
    bidding_strategy,
    target_conversions,
    skan_conversion_value,
    skan_source_app_id,
    skan_user_type,
    skan_ad_event_type,
    skan_ad_network_attribution_credit,
    skan_mapped_event,
    skan_event_value_low,
    skan_event_value_high,
    skan_event_value_mean,
    SUM(skan_postbacks) AS skan_postbacks,
    SUM(tcpi_skan_postbacks) AS tcpi_skan_postbacks,
    SUM(tcpi_advanced_skan_postbacks) AS tcpi_advanced_skan_postbacks,
    SUM(tcpa_skan_postbacks) AS tcpa_skan_postbacks,
    SUM(troas_skan_postbacks) AS troas_skan_postbacks,
    SUM(sum_null_values) AS sum_null_values,
    SUM(IF(bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST", sum_null_values, 0)) AS tcpi_sum_null_values,
    SUM(IF(bidding_strategy = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST", sum_null_values, 0))AS tcpi_advanced_sum_null_values,
    SUM(IF(bidding_strategy IN ("OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST","OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST"), sum_null_values, 0)) AS tcpa_sum_null_values,
    SUM(IF(bidding_strategy = "OPTIMIZE_RETURN_ON_ADVERTISING_SPEND", sum_null_values, 0)) AS troas_sum_null_values,
    SUM(sum_zero_values) AS sum_zero_values,
    SUM(IF(bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST", sum_zero_values, 0)) AS tcpi_sum_zero_values,
    SUM(IF(bidding_strategy = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST", sum_zero_values, 0)) AS tcpi_advanced_sum_zero_values,
    SUM(IF(bidding_strategy IN ("OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST","OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST"), sum_zero_values, 0)) AS tcpa_sum_zero_values,
    SUM(IF(bidding_strategy = "OPTIMIZE_RETURN_ON_ADVERTISING_SPEND", sum_zero_values, 0)) AS troas_sum_zero_values,
    SUM(sum_wo_null_zero_values) AS skan_values_wo_null_zero,
    SUM(IF(bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST", sum_wo_null_zero_values, 0)) AS tcpi_skan_values_wo_null_zero,
    SUM(IF(bidding_strategy = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST", sum_wo_null_zero_values, 0)) AS tcpi_advanced_skan_values_wo_null_zero,
    SUM(IF(bidding_strategy IN ("OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST","OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST"), sum_wo_null_zero_values, 0)) AS tcpa_skan_values_wo_null_zero,
    SUM(IF(bidding_strategy = "OPTIMIZE_RETURN_ON_ADVERTISING_SPEND", sum_wo_null_zero_values, 0)) AS troas_skan_values_wo_null_zero
  FROM PreparedData
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
  );
