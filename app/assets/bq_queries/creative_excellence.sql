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

-- Table for creative_excellence for on campaign_id level. Some information on
-- ad_group level is available. Contains aggregated performance data
-- (cost, installs, inapps) for the last 7 days.
CREATE OR REPLACE TABLE `{target_dataset}.creative_excellence`
AS (
  WITH
    -- Calculate ad_group level cost for the last 7 days
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
    CostDynamicsTable AS (
      SELECT
        PARSE_DATE('%Y-%m-%d', AP.date) AS day,
        AP.ad_group_id,
        ANY_VALUE(M.campaign_id) AS campaign_id,
        SUM(AP.cost) AS cost
      FROM `{bq_dataset}.ad_group_performance` AS AP
      LEFT JOIN MappingTable AS M
        USING(ad_group_id)
      WHERE
        PARSE_DATE('%Y-%m-%d', AP.date)
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        AND CURRENT_DATE()
      GROUP BY 1, 2
    ),
    AdGroupCostTable AS (
      SELECT
        ad_group_id,
        ANY_VALUE(campaign_id) AS campaign_id,
        COUNT(*) AS n_active_days,
        `{bq_dataset}.NormalizeMillis`(SUM(cost)) AS cost_last_7_days
      FROM CostDynamicsTable
      GROUP BY 1
    ),
    ConversionSplitTable AS (
      SELECT
        C.ad_group_id,
        ANY_VALUE(M.campaign_id) AS campaign_id,
        SUM(IF(C.conversion_category = 'DOWNLOAD', C.conversions, 0))
          AS installs,
        SUM(IF(C.conversion_category != 'DOWNLOAD', C.conversions, 0))
          AS inapps
      FROM `{bq_dataset}.ad_group_conversion_split` AS C
      LEFT JOIN `{bq_dataset}.account_campaign_ad_group_mapping` AS M
        USING(ad_group_id)
      WHERE
        PARSE_DATE('%Y-%m-%d', C.date)
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        AND CURRENT_DATE()
      GROUP BY 1
    ),
    -- Helper to identify campaign level bid and budget snapshot data
    -- for the last 7 days alongside corresponding lags
    BidBudget7DaysTable AS (
      SELECT
        day,
        campaign_id,
        budget_amount,
        LAG(budget_amount)
          OVER (
            PARTITION BY campaign_id ORDER BY day
          ) AS budget_amount_last_day,
        target_cpa,
        LAG(target_cpa)
          OVER (PARTITION BY campaign_id ORDER BY day) AS target_cpa_last_day,
        target_roas,
        LAG(target_roas)
          OVER (PARTITION BY campaign_id ORDER BY day) AS target_roas_last_day
      FROM `{bq_dataset}.bid_budgets_*`
      WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ),
    -- Average bid and budget data for each campaign for the last 7 days;
    -- counts how many times for a particual campaign bid and budget changes
    -- were greater than 20%
    BidBudgetAvg7DaysTable AS (
      SELECT
        campaign_id,
        SUM(IF(C.cost > B.budget_amount, 1, 0)) AS budget_overspend,
        COUNT(
          IF(
            SAFE_DIVIDE(B.budget_amount, B.budget_amount_last_day) > 1.2
              OR SAFE_DIVIDE(B.budget_amount, B.budget_amount_last_day) < 0.8,
            1,
            0)) AS dramatic_budget_changes,
        COUNT(
          IF(
            B.target_cpa > 0  -- check for campaigns with target_cpa bidding
              AND (
                SAFE_DIVIDE(B.target_cpa, B.target_cpa_last_day) > 1.2
                OR SAFE_DIVIDE(B.target_cpa, B.target_cpa_last_day) < 0.8),
            1,
            0)) AS dramatic_target_cpa_changes,
        COUNT(
          IF(
            B.target_roas > 0  -- check for campaigns with target_roas bidding
              AND (
                SAFE_DIVIDE(B.target_roas, B.target_roas_last_day) > 1.2
                OR SAFE_DIVIDE(B.target_roas, B.target_roas_last_day) < 0.8),
            1,
            0)) AS dramatic_target_roas_changes,
        AVG(`{bq_dataset}.NormalizeMillis`(B.budget_amount))
          AS average_budget_7_days,
        SUM(`{bq_dataset}.NormalizeMillis`(B.budget_amount)) AS sum_budget_7_days,
        COALESCE(
          AVG(`{bq_dataset}.NormalizeMillis`(B.target_cpa)),
          AVG(B.target_roas)) AS average_bid_7_days
      FROM BidBudget7DaysTable AS B
      LEFT JOIN CostDynamicsTable AS C
        USING (day, campaign_id)
      GROUP BY 1
    ),
    BudgetTable AS (
      SELECT
        campaign_id,
        ANY_VALUE(budget_amount) AS budget_amount,
        ANY_VALUE(target_cpa) AS target_cpa,
        ANY_VALUE(target_roas) AS target_roas
      FROM `{bq_dataset}.bid_budget`
      GROUP BY 1
    ),
    AssetStructureTable AS (
      SELECT
        ad_group_id,
        ANY_VALUE(ad_id) AS ad_id,
        ANY_VALUE(install_videos) AS install_videos,
        ANY_VALUE(install_images) AS install_images,
        ANY_VALUE(install_headlines) AS install_headlines,
        ANY_VALUE(install_descriptions) AS install_descriptions,
        ANY_VALUE(engagement_videos) AS engagement_videos,
        ANY_VALUE(engagement_images) AS engagement_images,
        ANY_VALUE(engagement_headlines) AS engagement_headlines,
        ANY_VALUE(engagement_descriptions) AS engagement_descriptions,
        ANY_VALUE(pre_registration_videos) AS pre_registration_videos,
        ANY_VALUE(pre_registration_images) AS pre_registration_images,
        ANY_VALUE(pre_registration_headlines) AS pre_registration_headlines,
        ANY_VALUE(pre_registration_descriptions)
          AS pre_registration_descriptions,
        ANY_VALUE(install_media_bundles) AS install_media_bundles,
      FROM `{bq_dataset}.asset_structure`
      GROUP BY 1
    ),
    AdStrengthTable AS (
      SELECT
        ad_id,
        ANY_VALUE(ad_strength) AS ad_strength
      FROM `{bq_dataset}.ad_strength`
      GROUP BY 1
    )
  SELECT
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
    '' AS firebase_bidding_status,
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    ACS.n_of_target_conversions,
    `{bq_dataset}.NormalizeMillis`(B.budget_amount) AS budget_amount,
    `{bq_dataset}.NormalizeMillis`(B.target_cpa) AS target_cpa,
    B.target_roas AS target_roas,
    -- For Installs campaigns the recommend budget amount it 50 times target_cpa
    -- for Action campaigns - 10 times target_cpa
    CASE
      WHEN
        ACS.bidding_strategy = 'Installs'
        AND SAFE_DIVIDE(B.budget_amount, B.target_cpa) >= 50
        THEN 'OK'
      WHEN
        ACS.bidding_strategy = 'Installs'
        AND SAFE_DIVIDE(B.budget_amount, B.target_cpa) < 50
        THEN '50x needed'
      WHEN
        ACS.bidding_strategy = 'Actions'
        AND SAFE_DIVIDE(B.budget_amount, B.target_cpa) >= 10
        THEN 'OK'
      WHEN
        ACS.bidding_strategy = 'Actions'
        AND SAFE_DIVIDE(B.budget_amount, B.target_cpa) < 10
        THEN '10x needed'
      ELSE 'Not Applicable'
      END AS enough_budget,
    -- number of active assets of a certain type
    `{bq_dataset}.GetNumberOfElements`(
      S.install_videos, S.engagement_videos, S.pre_registration_videos)
      AS n_videos,
    `{bq_dataset}.GetNumberOfElements`(
      S.install_images, S.engagement_images, S.pre_registration_images)
      AS n_images,
    `{bq_dataset}.GetNumberOfElements`(
      S.install_headlines, S.engagement_headlines, S.pre_registration_headlines)
      AS n_headlines,
    `{bq_dataset}.GetNumberOfElements`(
      S.install_descriptions,
      S.engagement_descriptions,
      S.pre_registration_descriptions) AS n_descriptions,
    ARRAY_LENGTH(SPLIT(S.install_media_bundles, '|')) - 1 AS n_html5,
    COALESCE(ADS.ad_strength, 'UNSPECIFIED') AS ad_strength,
    IFNULL(C.cost_last_7_days, 0) AS cost_last_7_days,
    IFNULL(
      IF(ACS.bidding_strategy = 'Installs', CS.installs, CS.inapps),
      0) AS conversions_last_7_days,
    CS.installs AS installs_last_7_days,
    CS.inapps AS inapps_last_7_days,
    CASE
      WHEN
        ACS.bidding_strategy = 'Installs'
        AND SUM(CS.installs) OVER (PARTITION BY CS.campaign_id) / 7 > 10
        THEN TRUE
      WHEN
        ACS.bidding_strategy IN ('Actions', 'Target ROAS')
        AND SUM(CS.inapps) OVER (PARTITION BY CS.campaign_id) / 7 > 10
        THEN TRUE
      ELSE FALSE
      END AS enough_conversions,
    BBA.average_budget_7_days AS average_budget_7_days,
    BBA.budget_overspend AS budget_overspend_7_days,
    BBA.budget_overspend / C.n_active_days AS budget_overspend_ratio,
    BBA.sum_budget_7_days AS sum_budget_7_days,
    BBA.average_bid_7_days AS average_bid_7_days,
    BBA.dramatic_budget_changes AS dramatic_budget_changes,
    COALESCE(
      BBA.dramatic_target_cpa_changes,
      BBA.dramatic_target_roas_changes) AS dramatic_bid_changes
  FROM MappingTable AS M
  LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
    ON M.campaign_id = ACS.campaign_id
  LEFT JOIN BudgetTable AS B
    ON M.campaign_id = B.campaign_id
  LEFT JOIN AssetStructureTable AS S
    ON M.ad_group_id = S.ad_group_id
  LEFT JOIN AdStrengthTable AS ADS
    ON S.ad_id = ADS.ad_id
  LEFT JOIN AdGroupCostTable AS C
    ON M.ad_group_id = C.ad_group_id
  LEFT JOIN ConversionSplitTable AS CS
    ON M.ad_group_id = CS.ad_group_id
  LEFT JOIN BidBudgetAvg7DaysTable AS BBA
    ON M.campaign_id = BBA.campaign_id
  WHERE C.cost_last_7_days > 0
);
