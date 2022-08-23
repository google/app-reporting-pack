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

"""Module for defining queries used in the project."""

from gaarf.base_query import BaseQuery


class ConversionLagQuery(BaseQuery):
    """Fetches all_conversions by network and conversion_id."""
    def __init__(self, start_date, end_date):
        self.query_text = f"""
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
