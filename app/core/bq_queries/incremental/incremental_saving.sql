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

-- Save ad_group performance data for a single day
CREATE OR REPLACE TABLE `{target_dataset}.ad_group_network_split_{yesterday_iso}`
AS (
  SELECT
    day,
    account_id,
    account_name,
    ocid,
    currency,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_sub_type,
    geos,
    languages,
    app_id,
    app_store,
    bidding_strategy,
    target_conversions,
    firebase_bidding_status,
    ad_group_id,
    ad_group_name,
    ad_group_status,
    network,
    clicks,
    impressions,
    cost,
    conversions,
    installs,
    installs_adjusted,
    inapps,
    inapps_adjusted,
    view_through_conversions,
    video_views,
    conversions_value,
    {% for custom_conversion in custom_conversions %}
    {% for conversion_alias, conversion_name in custom_conversion.items() %}
      conversions_{{conversion_alias}},
      conversions_value_{{conversion_alias}},
    {% endfor %}
    {% endfor %}
  FROM `{target_dataset}.ad_group_network_split_{date_iso}`
  WHERE day <= '{start_date}'
);

-- Save ad_group performance data for the current fetch
CREATE OR REPLACE TABLE `{target_dataset}.ad_group_network_split_{date_iso}`
AS (
  SELECT
    day,
    account_id,
    account_name,
    ocid,
    currency,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_sub_type,
    geos,
    languages,
    app_id,
    app_store,
    bidding_strategy,
    target_conversions,
    firebase_bidding_status,
    ad_group_id,
    ad_group_name,
    ad_group_status,
    network,
    clicks,
    impressions,
    cost,
    conversions,
    installs,
    installs_adjusted,
    inapps,
    inapps_adjusted,
    view_through_conversions,
    video_views,
    conversions_value,
    {% for custom_conversion in custom_conversions %}
    {% for conversion_alias, conversion_name in custom_conversion.items() %}
      conversions_{{conversion_alias}},
      conversions_value_{{conversion_alias}},
    {% endfor %}
    {% endfor %}
  FROM `{target_dataset}.ad_group_network_split_{date_iso}`
  WHERE day > '{start_date}'
);
