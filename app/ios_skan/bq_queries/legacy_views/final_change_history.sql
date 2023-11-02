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

CREATE OR REPLACE VIEW `{legacy_dataset}.change_history` AS
SELECT
    Day,
    account_name AS AccountName,
    ocid AS CID,
    account_id,
    Currency,
    campaign_sub_type AS CampaignSubType,
    campaign_name AS CampaignName,
    campaign_id AS CampaignID,
    CASE app_store
        WHEN 'GOOGLE_APP_STORE' THEN 'Google Play'
        WHEN 'APPLE_APP_STORE' THEN 'App Store'
        ELSE 'Other'
    END AS Store,
    app_id AS AppId,
    CASE campaign_status
        WHEN "ENABLED" THEN "Active"
        WHEN "PAUSED" THEN "Paused"
        WHEN "REMOVED" THEN "Deleted"
    END AS CampaignStatus,
    CASE
        WHEN bidding_strategy = "Installs" THEN "UAC Installs"
        WHEN bidding_strategy = "Installs Advanced" THEN "UAC Installs Advanced"
        WHEN bidding_strategy = "Actions" THEN "UAC Actions"
        WHEN bidding_strategy = "Maximize Conversions" THEN "UAC Maximize Conversions"
        WHEN bidding_strategy = "Target ROAS" THEN "UAC ROAS"
        WHEN bidding_strategy = "Preregistrations" THEN "UAC Pre-Registrations"
        ELSE "Unknown"
    END AS UACType,
    start_date AS StartDate,
    FALSE AS VideoModeOnly,
    -- firebase_bidding_status,
    target_conversions AS TargetConversions,
    geos AS country_code,
    languages AS Language,
    REGEXP_CONTAINS(languages, r"en|All") AS English,
    n_of_target_conversions,
    "" AS cannibalization_hash_strict,
    "" AS cannibalization_hash_medium,
    "" AS cannibalization_hash_flex,
    "" AS cannibalization_hash_broad,
    days_since_start_date AS Ndays,
    current_budget_amount AS CurrentBudget,
    target_cpa AS targetCPA,
    current_target_cpa AS currentTargetCPA,
    current_target_cpa AS LastTCPA,
    current_target_cpa AS CurrentTCPA,
    target_roas AS targetROAS,
    current_target_roas AS currentTROAS,
    budget AS Budget,
    bid_changes AS BidChanges,
    budget_changes AS BudgetChanges,
    clicks,
    impressions,
    cost,
    installs,
    installs_adjusted,
    inapps,
    inapps_adjusted,
    conversions_value AS conversion_value,
    conversions,
    conversions_adjusted,
    is_budget_limited AS Limited,
    is_budget_overshooting AS Overshooting,
    is_budget_underspend AS Underspend,
    is_cpa_overshooting AS CPAOvershooting,
    geo_changes AS GeoChanges,
    image_changes AS ImageChanges,
    text_changes AS TextChanges,
    html5_changes AS HTML5Changes,
    video_changes AS VideoChanges,
    0 AS ad_groups_added,
    0 AS ad_groups_resumed,
    0 AS ad_groups_paused,
    0 AS ad_groups_deleted,
    SAFE_DIVIDE(cost, conversions) AS CPA,
    S.skan_postbacks
FROM `{target_dataset}.change_history`
LEFT JOIN (
    SELECT
        PARSE_DATE("%Y-%m-%d", CAST(date AS STRING)) AS day,
        campaign_id,
        SUM(skan_postbacks) AS skan_postbacks
    FROM `{bq_dataset}.ios_campaign_skan_performance`
    GROUP BY 1, 2) AS S
USING(day, campaign_id);
