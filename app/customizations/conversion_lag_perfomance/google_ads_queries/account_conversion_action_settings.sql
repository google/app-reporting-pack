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
    customer.id AS customer_id,
    conversion_action.id AS conversion_id,
    conversion_action.third_party_app_analytics_settings.event_name AS third_party_app_analytics_event_name,
    conversion_action.third_party_app_analytics_settings.provider_name AS third_party_app_analytics_provider_name,
    conversion_action.firebase_settings.event_name AS firebase_event_name,
    conversion_action.firebase_settings.property_name AS firebase_property_name,
    conversion_action.status AS conversion_event_status,
    conversion_action.type AS conversion_event_type,
    conversion_action.counting_type AS conversion_event_counting_type,
    conversion_action.include_in_conversions_metric AS include_event_in_conversions_metric,
    conversion_action.name AS conversion_event_name,
    conversion_action.view_through_lookback_window_days AS conversion_event_view_through_lookback_window,
    conversion_action.click_through_lookback_window_days AS conversion_event_click_through_lookback_window
FROM
    conversion_action
WHERE 
    segments.date >= "{start_date}"
	AND segments.date <= "{end_date}"