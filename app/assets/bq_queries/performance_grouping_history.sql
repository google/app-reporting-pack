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

-- Contains dynamics by asset performance grouping status.
CREATE OR REPLACE TABLE `{target_dataset}.performance_grouping`
AS (
  WITH
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
    )
  SELECT
    PG.day,
    M.account_id,
    M.account_name,
    M.ocid,
    M.currency,
    M.campaign_id,
    M.campaign_name,
    M.campaign_status,
    ACS.campaign_sub_type,
    ACS.app_id,
    ACS.app_store,
    ACS.bidding_strategy,
    ACS.target_conversions,
    ACS.start_date AS start_date,
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    PG.performance_label,
    G.geos,
    G.languages,
    COUNT(*) AS N
  FROM `{bq_dataset}.performance_grouping_statuses_*` AS PG 
  LEFT JOIN MappingTable AS M
    ON PG.ad_group_id = M.ad_group_id
  LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
    ON M.campaign_id = ACS.campaign_id
  LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
    ON M.campaign_id = G.campaign_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
);
