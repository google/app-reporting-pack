# Copyright 2024 Google LLC
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

"""Module for defining queries used in the project."""

from gaarf import base_query


class DateRangeQuery(base_query.BaseQuery):
  """Defines common class of queries with customizable date range."""

  def __init__(self, start_date: str, end_date: str) -> None:
    """Replaces start and end date in query with supplied current_values."""
    self.start_date = start_date
    self.end_date = end_date


class ConversionLagQuery(DateRangeQuery):
  """Fetches all_conversions by network and conversion_id."""

  query_text = """
      SELECT
        campaign.id AS campaign_id,
        segments.ad_network_type AS network,
        segments.conversion_action~0 AS conversion_id,
        segments.conversion_action_name AS conversion_name,
        segments.conversion_lag_bucket AS conversion_lag_bucket,
        metrics.all_conversions AS all_conversions
      FROM campaign
      WHERE
        segments.date >= '{start_date}'
        AND segments.date <= '{end_date}'
      """


class ChangeHistory(DateRangeQuery):
  """Fetches change history for bids and budgets for app campaigns."""

  query_text = """
      SELECT
        change_event.change_date_time AS change_date,
        campaign.id AS campaign_id,
        change_event.old_resource:campaign_budget.amount_micros
          AS old_budget_amount,
        change_event.new_resource:campaign_budget.amount_micros
          AS new_budget_amount,
        change_event.old_resource:campaign.target_cpa.target_cpa_micros
          AS old_target_cpa,
        change_event.new_resource:campaign.target_cpa.target_cpa_micros
          AS new_target_cpa,
        change_event.old_resource:campaign.target_roas.target_roas
          AS old_target_roas,
        change_event.new_resource:campaign.target_roas.target_roas
          AS new_target_roas
      FROM change_event
      WHERE
        campaign.advertising_channel_type = MULTI_CHANNEL
        AND change_event.change_date_time >= '{start_date}'
        AND change_event.change_date_time <= '{end_date}'
        AND change_event.change_resource_type IN (CAMPAIGN_BUDGET, CAMPAIGN)
      LIMIT 10000
      """


class BidsBudgetsActiveCampaigns(base_query.BaseQuery):
  """Fetches bids and budget values for active app campaigns."""

  query_text = """
      SELECT
        campaign.id AS campaign_id,
        campaign_budget.amount_micros AS budget_amount,
        campaign.target_cpa.target_cpa_micros AS target_cpa,
        campaign.target_roas.target_roas AS target_roas
      FROM campaign
      WHERE
        campaign.advertising_channel_type = MULTI_CHANNEL
        AND campaign.status = ENABLED
      """


class BidsBudgetsInactiveCampaigns(DateRangeQuery):
  """Fetches bids and budgets for previously active app campaigns."""

  query_text = """
      SELECT
        campaign.id AS campaign_id,
        campaign_budget.amount_micros AS budget_amount,
        campaign.target_cpa.target_cpa_micros AS target_cpa,
        campaign.target_roas.target_roas AS target_roas
      FROM campaign
      WHERE
        campaign.advertising_channel_type = MULTI_CHANNEL
        AND campaign.status != ENABLED
        AND segments.date >= '{start_date}'
        AND segments.date <= '{end_date}'
        AND metrics.impressions > 0
      """


class CampaignsWithSpend(DateRangeQuery):
  """Fetches campaign_ids with non-zero impressions."""

  query_text = """
      SELECT
        campaign.id
      FROM campaign
      WHERE
        campaign.advertising_channel_type = MULTI_CHANNEL
        AND segments.date >= '{start_date}'
        AND segments.date <= '{end_date}'
        AND metrics.impressions > 0
  """
