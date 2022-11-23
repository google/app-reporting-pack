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

-- Get state information on each asset - status, performance labels, policy_summary
SELECT
    ad_group.id AS ad_group_id,
    asset.id AS asset_id,
    ad_group_ad_asset_view.field_type AS field_type,
    ad_group_ad_asset_view.enabled AS enabled,
    ad_group_ad_asset_view.performance_label AS performance_label,
    ad_group_ad_asset_view.policy_summary:review_status AS review_status,
    ad_group_ad_asset_view.policy_summary:approval_status AS approval_status,
    ad_group_ad_asset_view.policy_summary:policy_topic_entries.type AS policy_topic_type,
    ad_group_ad_asset_view.policy_summary:policy_topic_entries.topic AS policy_topics
FROM ad_group_ad_asset_view
WHERE ad_group_ad_asset_view.enabled = True
