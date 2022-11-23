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

CREATE OR REPLACE VIEW {legacy_dataset}.uac_network_split AS
SELECT
    Day,
    campaign_id AS CampaignID,
    account_name AS AccountName,
    app_id AS AppId,
    app_store AS Store,
    "" AS CID,
    account_id,
    campaign_sub_type AS CampaignSubType,
    campaign_name AS CampaignName,
    campaign_status AS CampaignStatus,
    ad_group_name AS AdGroupName,
    ad_group_id AS AdGroupId,
    Network,
    "" AS cannibalization_hash_strict,
    "" AS cannibalization_hash_medium,
    "" AS cannibalization_hash_flex,
    "" AS cannibalization_hash_broad,
    geos AS country_code,
    languages AS Language,
    clicks,
    impressions,
    cost,
    conversions,
    installs,
    installs_adjusted,
    inapps,
    inapps_adjusted,
    video_views,
    conversions_value AS conversion_value
FROM {target_dataset}.ad_group_network_split;
