# Copyright 2022 Google LLC
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

CREATE OR REPLACE VIEW {legacy_dataset}.uac_cannibalisation AS
WITH
  campaign_info AS (
    SELECT
      campaign_id,
      campaign_name,
      app_id,
      app_store,
      bidding_strategy,
      campaign_status,
      geos AS country_code,
      languages,
      geo_ids AS criteria_id,
      CASE
        WHEN target_conversions IS NULL THEN "Installs"
        ELSE target_conversions
        END AS target_conversions
    FROM
      {bq_dataset}.AppCampaignSettingsView
    JOIN
      (SELECT DISTINCT campaign_id, campaign_status, campaign_name FROM 
          {bq_dataset}.account_campaign_ad_group_mapping
      ) USING (campaign_id)
    LEFT JOIN
      {bq_dataset}.GeoLanguageView
      USING(campaign_id)
  ),
  campaigns_of_interest AS (
    SELECT
      campaign_id,
      ROUND(SUM(cost / 1000000), 2) AS cost
    FROM
        {bq_dataset}.ad_group_performance
    JOIN {bq_dataset}.account_campaign_ad_group_mapping
        USING(ad_group_id)
    WHERE
      DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
      AND Cost IS NOT NULL
    GROUP BY
      1
  ),
  ready_campaigns AS (
    SELECT
      country_code,
      languages,
      COALESCE(criteria_id, "All") AS criteria_id,
      app_id,
      app_store,
      bidding_strategy,
      target_conversions,
      campaign_id,
      COUNT(campaign_id) as NCampaigns
    FROM
      campaign_info
    WHERE
      campaign_id IN (SELECT campaign_id from campaigns_of_interest)
    GROUP BY
      1, 2, 3, 4, 5, 6, 7, 8
  ),
  broad AS (
    SELECT DISTINCT
      country_code,
      criteria_id,
      app_store,
      languages
    FROM
      ready_campaigns
  ),
  hashing_broad AS (
    SELECT
      country_code,
      criteria_id,
      app_store,
      languages,
      TO_HEX(
          MD5(
              CONCAT(
                  country_code,
                  CAST(criteria_id AS STRING),
                  CAST(languages AS STRING),
                  CAST(app_store AS STRING)
                )
            )
        ) AS cannibalization_hash_broad
    FROM
      broad
  ),
  strict AS (
    SELECT DISTINCT
      country_code,
      criteria_id,
      languages,
      app_id,
      bidding_strategy,
      target_conversions
    FROM
      ready_campaigns),
  hashing_strict AS (
    SELECT
      country_code,
      criteria_id,
      languages,
      app_id,
      bidding_strategy,
      target_conversions,
      TO_HEX(
          MD5(
              CONCAT(
                  country_code,
                  CAST(criteria_id AS STRING),
                  CAST(languages AS STRING),
                  CAST(app_id AS STRING),
                  CAST(bidding_strategy AS STRING),
                  target_conversions
              )
            )
          ) AS cannibalization_hash_strict
    FROM
      strict
  ),
  medium AS (
    SELECT DISTINCT
      country_code,
      criteria_id,
      languages,
      app_id,
      bidding_strategy
    FROM
      ready_campaigns
  ),
  hashing_medium AS (
    SELECT
      country_code,
      criteria_id,
      languages,
      app_id,
      bidding_strategy,
      TO_HEX(
          MD5(
              CONCAT(
                  country_code,
                  CAST(criteria_id AS STRING),
                  CAST(languages AS STRING),
                  CAST(app_id AS STRING),
                  CAST(bidding_strategy AS STRING)
                )
            )
        ) AS cannibalization_hash_medium
    FROM
      medium
  ),
  flex AS (
    SELECT DISTINCT
      country_code,
      criteria_id,
      languages,
      app_id,
      bidding_strategy
    FROM
      ready_campaigns
  ),
  hashing_flex AS (
    SELECT
      country_code,
      criteria_id,
      languages,
      app_id,
      TO_HEX(
          MD5(
              CONCAT(
                  country_code,
                  CAST(criteria_id AS STRING),
                  CAST(languages AS STRING),
                  CAST(app_id AS STRING)
                )
            )
        ) AS cannibalization_hash_flex
    FROM
      flex
  ),
  cannibalisation AS (
    SELECT
      country_code,
      criteria_id,
      languages,
      app_id,
      bidding_strategy,
      target_conversions,
      campaign_id,
      app_store,
      cannibalization_hash_strict,
      cannibalization_hash_medium,
      cannibalization_hash_flex,
      cannibalization_hash_broad,
      COUNT(campaign_id) OVER (PARTITION BY
        country_code,
        criteria_id,
        languages,
        app_id,
        bidding_strategy,
        target_conversions) AS N_strict,
      COUNT(campaign_id) OVER (PARTITION BY
        country_code,
        criteria_id,
        languages,
        app_id,
        bidding_strategy) AS N_medium,
      COUNT(campaign_id) OVER (PARTITION BY
        country_code,
        criteria_id,
        languages,
        app_id,
        bidding_strategy) AS N_flex,
      COUNT(campaign_Id) OVER (PARTITION BY
        country_code,
        criteria_id,
        Languages,
        app_Store) AS N_broad
    FROM
      ready_campaigns
    -- broad
    LEFT JOIN
      hashing_broad
      USING(country_code, criteria_id, app_store, languages)
    -- strict
    LEFT JOIN
      hashing_strict
      USING(country_code, criteria_id, app_id, bidding_strategy, target_conversions, languages)
    LEFT JOIN
      hashing_medium
      -- medium
      USING(country_code, criteria_id, app_id, bidding_strategy, languages)
    -- flex
    LEFT JOIN
      hashing_flex
      USING(country_code, criteria_id, app_id, languages)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
  ),
  cannibalization_final_table
    AS (
      SELECT
        country_code,
        criteria_id,
        languages AS Language,
        app_id AS AppId,
        CASE  
            WHEN bidding_strategy = "Installs" THEN "UAC Installs"
            WHEN bidding_strategy = "Installs Advanced" THEN "UAC Installs Advanced"
            WHEN bidding_strategy = "Actions" THEN "UAC Actions"
            WHEN bidding_strategy = "Maximize Conversions" THEN "UAC Maximize Conversions"
            WHEN bidding_strategy = "Target ROAS" THEN "UAC ROAS"
            WHEN bidding_strategy = "Preregistrations" THEN "UAC Pre-Registrations"
            ELSE "Unknown"
        END AS optimizationGoal,
        target_conversions AS targetConversions,
        account_id,
        account_name AS AccountName,
        campaign_sub_type AS CampaignSubType,
        CAST(campaign_id AS STRING) AS CampaignID,
        campaign_name AS CampaignName,
        CONCAT(cannibalization_hash_strict, CAST(N_Strict AS STRING)) AS cannibalization_hash_strict,
        CONCAT(cannibalization_hash_medium, CAST(N_medium AS STRING)) AS cannibalization_hash_medium,
        CONCAT(cannibalization_hash_flex, CAST(N_flex AS STRING)) AS cannibalization_hash_flex,
        CONCAT(cannibalization_hash_broad, CAST(N_broad AS STRING)) AS cannibalization_hash_broad,
        N_strict,
        N_medium,
        N_flex,
        N_broad
      FROM
        cannibalisation
      LEFT JOIN
        (
          SELECT
            account_id,
            account_name,
            campaign_sub_type,
            campaign_id,
            campaign_name
          FROM
            {bq_dataset}.AppCampaignSettingsView
          JOIN
            (SELECT DISTINCT campaign_id, account_id, account_name, campaign_name FROM 
                {bq_dataset}.account_campaign_ad_group_mapping
            ) USING (campaign_id)

        )
        USING(campaign_id)
      WHERE
        N_broad > 1)
SELECT
  *,
  IF(cannibalization_hash_strict IS NULL, 0, N_strict - 1) AS same_event_app,
  IF(cannibalization_hash_medium IS NULL, 0, N_medium - 1) AS same_campaign_type_app,
  IF(cannibalization_hash_flex IS NULL, 0, N_flex - 1) AS same_geo_app,
  IF(cannibalization_hash_broad IS NULL, 0, N_broad - 1) AS same_store
FROM
 cannibalization_final_table;
