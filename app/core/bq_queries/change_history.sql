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

-- Contains daily changes (bids, budgets, assets, etc) on campaign level.
CREATE OR REPLACE TABLE `{target_dataset}.change_history`
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
        FROM `{bq_dataset}.ad_group_conversion_split` AS ConvSplit
        LEFT JOIN `{bq_dataset}.ConversionLagAdjustments` AS LagAdjustments
            ON PARSE_DATE("%Y-%m-%d", ConvSplit.date) = LagAdjustments.adjustment_date
                AND ConvSplit.network = LagAdjustments.network
                AND ConvSplit.conversion_id = LagAdjustments.conversion_id
        GROUP BY 1, 2, 3
    ),
    CampaignPerformance AS (
        SELECT
            PARSE_DATE("%Y-%m-%d", date) AS day,
            M.campaign_id,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            SUM(cost) AS cost,
            SUM(conversions) AS conversions,
            SUM(installs) AS installs,
            SUM(installs_adjusted) AS installs_adjusted,
            SUM(inapps) AS inapps,
            SUM(inapps_adjusted) AS inapps_adjusted,
            SUM(view_through_conversions) AS view_through_conversions,
            SUM(AP.conversions_value) AS conversions_value
        FROM `{bq_dataset}.ad_group_performance` AS AP
        LEFT JOIN ConversionsTable AS ConvSplit
            USING(date, ad_group_id, network)
        LEFT JOIN `{bq_dataset}.account_campaign_ad_group_mapping` AS M
            USING(ad_group_id)
        LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
          ON M.campaign_id = ACS.campaign_id
        GROUP BY 1, 2
    ),
    CampaignMapping AS (
        SELECT
            campaign_id,
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
    CP.day,
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
    ACS.start_date AS start_date,
    DATE_DIFF(
        CURRENT_DATE(),
        PARSE_DATE("%Y-%m-%d", ACS.start_date),
        DAY) AS days_since_start_date,
    ACS.n_of_target_conversions,
    `{bq_dataset}.NormalizeMillis`(B.budget_amount) AS current_budget_amount,
    `{bq_dataset}.NormalizeMillis`(B.target_cpa) AS current_target_cpa,
    B.target_roas AS current_target_roas,
    `{bq_dataset}.NormalizeMillis`(BidBudgetHistory.budget_amount) AS budget,
    `{bq_dataset}.NormalizeMillis`(BidBudgetHistory.target_cpa) AS target_cpa,
    BidBudgetHistory.target_roas AS target_roas,
    -- If cost for a given day is higher than allocated budget than is_budget_limited = 1
    IF(CP.cost > BidBudgetHistory.budget_amount, 1, 0) AS is_budget_limited,
    -- If cost for a given day is 20% higher than allocated budget than is_budget_overshooting = 1
    IF(CP.cost > BidBudgetHistory.budget_amount * 1.2, 1, 0) AS is_budget_overshooting,
    -- If cost for a given day is 50% lower than allocated budget than is_budget_underspend = 1
    IF(CP.cost < BidBudgetHistory.budget_amount * 0.5, 1, 0) AS is_budget_underspend,
    -- If CPA for a given day is 10% higher than target_cpa than is_cpa_overshooting = 1
    IF(SAFE_DIVIDE(CP.cost, CP.conversions) > BidBudgetHistory.target_cpa * 1.1, 1, 0) AS is_cpa_overshooting,
    0 AS geo_changes,
    0 AS ad_group_added,
    0 AS ad_group_resumed,
    0 AS ad_group_paused,
    0 AS ad_group_deleted,
    CASE
        WHEN BidBudgetHistory.target_cpa = LAG(BidBudgetHistory.target_cpa)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC)
        OR LAG(BidBudgetHistory.target_cpa)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC) IS NULL
        THEN 0
    ELSE 1
    END AS bid_changes,
    CASE
        WHEN BidBudgetHistory.budget_amount = LAG(BidBudgetHistory.budget_amount)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC)
        OR LAG(BidBudgetHistory.budget_amount)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC) IS NULL
        THEN 0
    ELSE 1
    END AS budget_changes,
    CP.clicks,
    CP.impressions,
    `{bq_dataset}.NormalizeMillis`(CP.cost) AS cost,
    IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
        CP.installs, CP.inapps) AS conversions,
    IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
        CP.installs_adjusted, CP.inapps_adjusted) AS conversions_adjusted,
    CP.installs,
    CP.installs_adjusted,
    CP.inapps,
    CP.inapps_adjusted,
    CP.view_through_conversions,
    CP.conversions_value,
FROM CampaignPerformance AS CP
LEFT JOIN CampaignMapping AS M
    ON CP.campaign_id = M.campaign_id
LEFT JOIN `{bq_dataset}.bid_budget` AS B
    ON CP.campaign_id = B.campaign_id
LEFT JOIN `{bq_dataset}.bid_budgets_*` AS BidBudgetHistory
    ON M.campaign_id = BidBudgetHistory.campaign_id
        AND CP.day = BidBudgetHistory.day
LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
);
