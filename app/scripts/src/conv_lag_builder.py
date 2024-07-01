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

"""Module for getting conversion lag adjustment table.

Conversion lag adjustment tables contains network specific lags (from 1 till
max 90) for each conversion_id that can be applied to any performance dataset.
"""

import os
from functools import reduce

import gaarf
import pandas as pd


class ConversionLagBuilder:
  """Class for generating conversion lag adjustment table."""

  def __init__(self, lag_data: pd.DataFrame, base_groupby: list[str]) -> None:
    """Initilizes ConversionLagBuilder with lag_data and groupby bases.

    Args:
      lag_data:
        DataFrame with number of conversions for each lag bucket for network,
          campaign_id, and conversion_id.
      base_groupby:
        Attributes of lag_data to control the granularity of calculating
          conversion lags.
    """
    self.lag_data = lag_data
    self.group_by = base_groupby

  def _read_lag_enums(self) -> pd.DataFrame:
    dirname = os.path.dirname(__file__)
    return pd.read_csv(
      os.path.join(dirname, '../data/conversion_lag_mapping.csv')
    )

  def calculate_reference_values(self) -> gaarf.report.GaarfReport:
    """Method for getting conversion lag adjustment table.

    Returns:
       DataFrame with conversion lag adjustment table.
    """
    joined_data = self.join_lag_data_and_enums(self.lag_data)
    grouped = joined_data.groupby(self.group_by)
    conversion_lag_table = []
    for _, df_group in grouped:
      grouped_data = self.calculate_conversions_by_name_network_lag(df_group)
      if grouped_data.empty:
        continue
      cumulative_data = self.calculate_cumulative_sum_ordered_by_n(grouped_data)
      expanded_data = self.expand_and_join_lags(cumulative_data)
      incremental_lag = self.calculate_incremental_lag(expanded_data)
      conversion_lag_table.append(incremental_lag)
    return gaarf.report.GaarfReport.from_pandas(
      reduce(
        lambda left, right: pd.concat([left, right], ignore_index=True),
        conversion_lag_table,
      )
    )

  def join_lag_data_and_enums(self, lag_data: pd.DataFrame) -> pd.DataFrame:
    """Joins conversion lag data enums from Google Ads API with INT values."""
    return pd.merge(
      lag_data, self._read_lag_enums(), on='conversion_lag_bucket', how='left'
    )

  def calculate_conversions_by_name_network_lag(
    self, joined_data: pd.DataFrame
  ) -> pd.DataFrame:
    """Sums and sorts all_conversions by group_by parameter for each lag."""
    return (
      joined_data.groupby(self.group_by + ['lag_number'], as_index=False)
      .agg({'all_conversions': 'sum'})
      .sort_values(by=self.group_by + ['lag_number'])
    )

  def calculate_cumulative_sum_ordered_by_n(
    self, grouped_data: pd.DataFrame
  ) -> pd.DataFrame:
    """Generates cumulative share of each conversion lag day within a bucket.

    Adds several important metrics to the DataFrame:
        * pct_conv - cumulative percentage of conversions on N-th day
        * lag_distance - difference in number of days between current
            lag bucket and the previous one (i.e. if lag bucket is
            `SIXTY_TO_NINETY_DAYS` and the previous value is
            `FORTY_FIVE_TO_SIXTY_DAYS` the lag_distance would be 30.
        * daily_incremental_lag - difference in pct_conv between current
            and previous value of the lag bucket
    """
    grouped_data['cumsum'] = grouped_data.groupby(
      self.group_by, as_index=False
    )['all_conversions'].cumsum()
    total_conversions_by_group = grouped_data.groupby(
      self.group_by, as_index=False
    )['all_conversions'].sum()
    new_ds = pd.merge(
      grouped_data[self.group_by + ['lag_number', 'cumsum']],
      total_conversions_by_group[self.group_by + ['all_conversions']],
      on=self.group_by,
      how='left',
    )
    new_ds['pct_conv'] = new_ds['cumsum'] / new_ds['all_conversions']
    new_ds['shifted_lag_number'] = new_ds['lag_number'].shift(1)
    new_ds['shifted_pct_conv'] = new_ds['pct_conv'].shift(1)
    new_ds['lag_distance'] = new_ds['lag_number'] - new_ds['shifted_lag_number']
    new_ds['lag_distance'] = new_ds['lag_distance'].fillna(1)
    new_ds['daily_incremental_lag'] = (
      new_ds['pct_conv'] - new_ds['shifted_pct_conv']
    )
    new_ds['daily_incremental_lag'] = new_ds['daily_incremental_lag'].fillna(
      new_ds['pct_conv'][0]
    )
    return new_ds

  def expand_and_join_lags(self, lag_data: pd.DataFrame) -> pd.DataFrame:
    """Expand DataFrame by repeating rows with lag_distance > 1.

    Number of repeats are equal to lag_distance.
    """
    return lag_data.loc[lag_data.index.repeat(lag_data.lag_distance)]

  def calculate_incremental_lag(
    self, expanded_data: pd.DataFrame
  ) -> pd.DataFrame:
    """Get cumulative incremental value of lag for each day.

    Each day of conversion lag is recreated, for each day of
    conversion lag the incremental cumulative value is calculated.
    """
    expanded_data['incremental_lag'] = (
      expanded_data['daily_incremental_lag'] / expanded_data['lag_distance']
    )
    expanded_data['lag_adjustment'] = expanded_data.groupby(
      self.group_by, as_index=False
    )['incremental_lag'].cumsum()
    expanded_data['lag_day'] = (
      expanded_data.groupby(self.group_by).cumcount() + 1
    )
    return expanded_data[self.group_by + ['lag_day', 'lag_adjustment']]
