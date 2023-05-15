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

SELECT
    segments.date AS date,
    campaign.id AS campaign_id,
    segments.sk_ad_network_conversion_value AS skan_conversion_value,
    segments.sk_ad_network_source_app.sk_ad_network_source_app_id AS skan_source_app_id,
    segments.sk_ad_network_user_type AS skan_user_type,
    segments.sk_ad_network_ad_event_type AS skan_ad_event_type,
    segments.sk_ad_network_attribution_credit AS skan_ad_network_attribution_credit,
    metrics.sk_ad_network_conversions AS skan_postbacks
FROM campaign
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND campaign.app_campaign_setting.app_store = "APPLE_APP_STORE"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
