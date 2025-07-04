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
    segments.ad_network_type AS network,
    ad_group.id AS ad_group_id,
    age_range_view.resource_name AS age_range,
    metrics.clicks AS clicks,
    metrics.impressions AS impressions,
    metrics.engagements AS engagements,
    metrics.interactions AS interactions,
    metrics.video_views AS video_views,
    metrics.video_quartile_p25_rate AS video_quartile_p25_rate,
    metrics.video_quartile_p50_rate AS video_quartile_p50_rate,
    metrics.video_quartile_p75_rate AS video_quartile_p75_rate,
    metrics.video_quartile_p100_rate AS video_quartile_p100_rate,
    metrics.view_through_conversions AS  view_through_conversions,
    metrics.cost_micros AS cost,
    metrics.conversions_value AS conversions_value
FROM
    age_range_view
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"