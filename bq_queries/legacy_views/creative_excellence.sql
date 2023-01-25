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

CREATE OR REPLACE VIEW {legacy_dataset}.uachygiene AS
SELECT
    "" AS CID,
    account_id,
    account_name AS AccountName,
    campaign_sub_type AS CampaignSubType,
    campaign_id AS CampaignID,
    campaign_name AS CampaignName,
    CASE campaign_status
        WHEN "ENABLED" THEN "Active"
        WHEN "PAUSED" THEN "Paused"
        WHEN "REMOVED" THEN "Deleted"
    END AS CampaignStatus,
    ad_group_id AS AdGroupId,
    ad_group_name AS AdGroup,
    CASE ad_group_status
        WHEN "ENABLED" THEN "Active"
        WHEN "PAUSED" THEN "Paused"
        WHEN "REMOVED" THEN "Deleted"
    END  AS AdGroupStatus,
    CASE  
        WHEN bidding_strategy = "Installs" THEN "UAC Installs"
        WHEN bidding_strategy = "Installs Advanced" THEN "UAC Installs Advanced"
        WHEN bidding_strategy = "Actions" THEN "UAC Actions"
        WHEN bidding_strategy = "Maximize Conversions" THEN "UAC Maximize Conversions"
        WHEN bidding_strategy = "Target ROAS" THEN "UAC ROAS"
        WHEN bidding_strategy = "Preregistrations" THEN "UAC Pre-Registrations"
        ELSE "Unknown"
    END AS UACType,
    firebase_bidding_status,
    app_store AS Store,
    app_id AS App,
    target_conversions AS ConvNames,
    n_of_target_conversions,
    budget_amount AS BudgetAmount,
    target_cpa AS TargetCPA,
    enough_budget AS EnoughBudget,
    conversions_last_7_days AS conversions,
    enough_conversions AS EnoughConversions,
    n_images,
    n_videos,
    n_headlines,
    n_descriptions AS n_descriptions,
    n_html5 AS n_html5,
    FALSE AS VideoModeOnly,
    FALSE AS has_portrait_video,
    FALSE AS has_landscape_video,
    FALSE AS has_landscape_image,
    FALSE AS creative_excellence,
    ad_strength,
    cost_last_7_days AS Cost,
    installs_last_7_days AS Installs,
    inapps_last_7_days AS InApps,
    budget_overspend_7_days AS NDaysLimited,
    budget_overspend_ratio AS LimitRatio,
    sum_budget_7_days AS Budget7Days,
    cost_last_7_days AS Cost7Days,
    average_budget_7_days AS AverageBudget7Days,
    average_bid_7_days AS AverageBid7Days,
    dramatic_bid_changes AS DramaticBidChanges,
    dramatic_budget_changes AS DramaticBudgetChanges
FROM {target_dataset}.creative_excellence;
