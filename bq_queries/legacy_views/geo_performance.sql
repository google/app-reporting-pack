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

CREATE OR REPLACE VIEW {legacy_dataset}.geo_performance AS
SELECT
    Day,
    campaign_id AS CampaignID,
    account_name AS AccountName,
    "" AS CID,
    app_id AS AppId,
    account_id,
    campaign_sub_type AS CampaignSubType,
    campaign_name AS CampaignName,
    CASE campaign_status
        WHEN "ENABLED" THEN "Active"
        WHEN "PAUSED" THEN "Paused"
        WHEN "REMOVED" THEN "Deleted"
    END AS CampaignStatus,
    ad_group_name AS AdGroupName,
    ad_group_id AS AdGroupId,
    country_code AS Country,
    target_conversions AS TargetConversions,
    Network,
    clicks,
    impressions,
    cost,
    installs,
    inapps,
    conversions_value AS conversion_value
FROM {target_dataset}.geo_performance;
