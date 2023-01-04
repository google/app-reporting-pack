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

SELECT
    segments.date AS date,
    segments.ad_network_type AS network,
    ad_group.id AS ad_group_id,
    asset.id AS asset_id,
    ad_group_ad_asset_view.field_type AS field_type,
    segments.conversion_action~0 AS conversion_id,
    metrics.conversions AS conversions,
    metrics.all_conversions AS all_conversions,
    metrics.conversions_value AS conversions_value
FROM ad_group_ad_asset_view
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
