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
"""Entrypoint for performing various backfilling operations."""

import argparse
import datetime
import functools
import itertools
import logging
from bisect import bisect_left
from collections.abc import Sequence
from typing import Generator

import gaarf
import numpy as np
import pandas as pd
from gaarf import api_clients
from gaarf.cli import utils as gaarf_utils
from gaarf.executors import bq_executor
from gaarf.io.writers import bigquery_writer
from google.api_core import exceptions as google_api_exceptions
from src import queries


def get_new_date_for_missing_incremental_snapshots(
  bigquery_executor: bq_executor.BigQueryExecutor,
  bq_dataset: str,
  table_name: str,
) -> None:
  """Checks for missing snapshots and outputs new start date.

  If there's a gap in data we need to remove the table with the problematic
  TABLE_SUFFIX and find a new start_date that should be use by the
  application when running data fetching.

  Args:
      bigquery_executor: Executor to run big query queries.
      bq_dataset: BQ dataset to get data from.
      table_name: BQ table that are checked for incremental snapshots.
  """
  start_date = ''
  base_table_id = f'{bigquery_executor.project_id}.{bq_dataset}.{table_name}'
  base_output_table_id = (
    f'{bigquery_executor.project_id}.{bq_dataset}_output.{table_name}'
  )
  try:
    result = bigquery_executor.execute(
      'missing_incremental_snapshot',
      f'SELECT TABLE_SUFFIX, new_start_date FROM `{base_table_id}_missing`',
    )

    if bool(isinstance(result, pd.DataFrame)):
      start_date = result.new_start_date.squeeze().strftime('%Y-%m-%d')
      suffix = result.TABLE_SUFFIX.squeeze()
      delete_ddl = f'DROP TABLE `{base_output_table_id}_{suffix}`;'
      bigquery_executor.execute('drop_table', delete_ddl)
  except bq_executor.BigQueryExecutorException:
    pass
  print(start_date)


def restore_missing_bid_budgets(
  report_fetcher: gaarf.report_fetcher.AdsReportFetcher,
  customer_ids: Sequence[str],
  date_range: tuple[str, str],
) -> pd.DataFrame:
  """Fills missing gaps in bid and budget snapshots in BigQuery.

  Args:
    report_fetcher: Instantiated AdsReportFetcher to get data from Ads API.
    customer_ids: Accounts to check for bid / budgets events.
    date_range: Period for backfilling change history.

  Returns:
    DataFrame with restored change history.
  """
  # Change history can be fetched only for the last 28 days
  start_date, end_date = date_range
  dates = [
    date.strftime('%Y-%m-%d')
    for date in pd.date_range(start_date, end_date).to_pydatetime().tolist()
  ]

  logging.info(
    'Restoring change history for %d accounts: %s',
    len(customer_ids),
    customer_ids,
  )
  # extract change history report
  change_history = report_fetcher.fetch(
    queries.ChangeHistory(start_date, end_date), customer_ids
  ).to_pandas()
  campaign_ids = report_fetcher.fetch(
    queries.CampaignsWithSpend(start_date, end_date), customer_ids
  ).to_list(row_type='scalar', distinct=True)
  logging.info(
    'Change history will be restored for %d campaign_ids', len(campaign_ids)
  )

  current_bids_budgets = (
    (
      report_fetcher.fetch(queries.BidsBudgetsActiveCampaigns(), customer_ids)
      + report_fetcher.fetch(
        queries.BidsBudgetsInactiveCampaigns(start_date, end_date),
        customer_ids,
      )
    )
    .to_pandas()
    .drop_duplicates()
  )

  placeholders = _prepare_placeholders(campaign_ids, dates)

  return _restore_bid_budget_history(
    change_history,
    current_bids_budgets,
    placeholders,
  )


def _prepare_placeholders(
  campaign_ids: list[int], dates: list[str]
) -> pd.DataFrame:
  """Generates all possible combinations of dates and campaign_ids."""
  return pd.DataFrame(
    data=list(itertools.product(campaign_ids, dates)),
    columns=['campaign_id', 'day'],
  )


def _restore_bid_budget_history(
  change_history: pd.DataFrame,
  current_bids_budgets: pd.DataFrame,
  placeholders: pd.DataFrame,
) -> pd.DataFrame:
  """Restore change history for target roas, target_cpa, and budgets.

  Args:
    placeholders:
      Contains all possible combinations of dates for set of campaigns.
    change_history:
      Contains only changes in bid and budget events.
    current_bids_budgets:
      Contains last observable values of bid and budgets.

  Returns:
    DataFrame with merged restored change histories.
  """
  change_history_events = [
    _restore_event_history(
      placeholders, change_history, current_bids_budgets, event_type
    )
    for event_type in ('budget_amount', 'target_cpa', 'target_roas')
  ]
  return functools.reduce(
    lambda left, right: pd.merge(
      left, right, on=['day', 'campaign_id'], how='left'
    ),
    change_history_events,
  )


def _restore_event_history(
  placeholders: pd.DataFrame,
  change_history: pd.DataFrame,
  current_bids_budgets: pd.DataFrame,
  event_type: str,
) -> pd.DataFrame:
  """Restores gaps in change history for a single event_type.

  Args:
    placeholders:
      Contains all possible combinations of dates for set of campaigns.
    change_history:
      Contains only changes in bid and budget events.
    current_bids_budgets:
      Contains last observable values of bid and budgets.
    event_type:
      Specifies type of event to restore change history for.

  Returns:
    DataFrame with filled gaps in change history for a given event_type.
  """
  partial_history = _prepare_events(change_history, event_type)
  if not partial_history.empty:
    joined = pd.merge(
      placeholders, partial_history, on=['campaign_id', 'day'], how='left'
    )
    joined['filled_backward'] = joined.groupby('campaign_id')[
      'value_old'
    ].bfill()
    joined['filled_forward'] = joined.groupby('campaign_id')[
      'value_new'
    ].ffill()
    joined['filled_forward'] = joined.groupby('campaign_id')[
      'filled_forward'
    ].fillna(joined.pop('filled_backward'))
    joined = pd.merge(
      joined, current_bids_budgets, on='campaign_id', how='left'
    )
    joined[event_type] = np.where(
      joined['filled_forward'].isnull(),
      joined[event_type],
      joined['filled_forward'],
    )
  else:
    joined = pd.merge(
      placeholders, current_bids_budgets, on='campaign_id', how='left'
    )
  if event_type != 'target_roas':
    joined[event_type] = joined[event_type].astype(int)
  return joined[['day', 'campaign_id', event_type]]


def _prepare_events(
  change_history: pd.DataFrame, event_type: str
) -> pd.DataFrame:
  """Exacts change history events only for a given event_type.

  Args:
    change_history: Contains only changes in bid and budget events.
    event_type: Specifies type of event filter by.

  Returns:
    DataFrame with subset of change history for a given event_type.
  """
  events = change_history.loc[
    (change_history[f'old_{event_type}'] > 0)
    & (change_history[f'new_{event_type}'] > 0)
  ]
  if not events.empty:
    events = _format_partial_event_history(events, event_type)
    logging.info('%d %s events were found', len(events), event_type)
  else:
    logging.info('no %s events were found', event_type)
  return events


def _format_partial_event_history(
  events: pd.DataFrame, event_type: str
) -> pd.DataFrame:
  """Performs renaming and selecting last value for the date.

  Args:
    events: Change history data.
    event_type: Type of event to perform the formatting by.
  """
  events.loc[:, ('day')] = events['change_date'].str.split(expand=True)[0]
  events = events.rename(
    columns={
      f'old_{event_type}': 'value_old',
      f'new_{event_type}': 'value_new',
    }
  )[['day', 'campaign_id', 'value_old', 'value_new']]
  return events.groupby(['day', 'campaign_id']).last()


def _get_asset_cohorts_snapshots(
  bigquery_executor: bq_executor.BigQueryExecutor, bq_dataset: str
) -> set[datetime.date]:
  """Get suffixes of asset cohorts snapshots for the last 5 days.

  Args:
    bigquery_executor: Instantiated executor to write data to BigQuery.
    bq_dataset: BigQuery dataset to save data to.

  Returns:
    Days when snapshots are present.
  """
  snapshot_dates: set[datetime.date] = set()
  try:
    result = bigquery_executor.execute(
      'conversion_lags_snapshots',
      f"""
      SELECT DISTINCT _TABLE_SUFFIX AS day
      FROM
      `{bq_dataset}.conversion_lags_*`
      WHERE
        _TABLE_SUFFIX >= FORMAT_DATE(
           "%Y%m%d",DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY)
        )
        AND _TABLE_SUFFIX < FORMAT_DATE( "%Y%m%d",CURRENT_DATE)
      ORDER BY 1
     """,
    )
    has_snapshots = bool(isinstance(result, pd.DataFrame))
  except bq_executor.BigQueryExecutorException:
    logging.warning('failed to get data')
    return snapshot_dates
  if has_snapshots and (snapshots_days := set(result.day)):
    snapshot_dates = {
      datetime.datetime.strptime(date, '%Y%m%d').date()
      for date in snapshots_days
    }
  else:
    logging.warning('No available assert cohorts snapshots for the last 5 days')
  return snapshot_dates


def restore_missing_cohorts(
  snapshot_dates: set[datetime.date], bq_dataset: str
) -> Generator[str, None, None]:
  """Restores missing asset cohorts data.

  Cohorts snapshots can be restored only for the last 5 days.

  Args:
    snapshot_dates: Dates when cohort snapshots are present.
    bq_dataset: BigQuery dataset to save data to.

  Yields:
    Table id and query to restore the snapshot.
  """
  dates = {
    day.date()
    for day in pd.date_range(min(snapshot_dates), max(snapshot_dates))
    .to_pydatetime()
    .tolist()
  }

  if not (missing_dates := list(dates.difference(snapshot_dates))):
    return
  if not missing_dates:
    logging.info('No asset cohort snapshots to backfill')
    return
  snapshot_dates = sorted(snapshot_dates)
  for date in sorted(missing_dates):
    index = bisect_left(snapshot_dates, date)
    if index == 0:
      continue
    last_available_date = snapshot_dates[index - 1]
    date_diff = (date - last_available_date).days
    table_id = f'{bq_dataset}.conversion_lags_{date.strftime("%Y%m%d")}'
    last_available_date_table_id = last_available_date.strftime('%Y%m%d')
    yield (
      table_id,
      f"""
      CREATE OR REPLACE TABLE `{table_id}`
      AS
      SELECT
          day_of_interaction,
          lag+{date_diff} AS lag,
          ad_group_id,
          asset_id,
          field_type,
          network AS network,
          installs,
          inapps,
          view_through_conversions,
          conversions_value
      FROM `{bq_dataset}.conversion_lags_{last_available_date_table_id}`
     """,
    )


def save_restored_asset_cohort(
  bigquery_executor: bq_executor.BigQueryExecutor, query: str, table_id: str
) -> None:
  """Execute query for backfilling asset cohort snapshot.

  Cohorts snapshots can be restored only for the last 5 days.

  Args:
    bigquery_executor: Instantiated executor to write data to BigQuery.
    query: Query for snapshot backfilling.
    table_id: Full name of the table when snapshot saved to.
  """
  try:
    bigquery_executor.execute('restore_conversion_lag_snapshot', query)
    logging.info("table '%s' has been created", table_id)
  except bq_executor.BigQueryExecutorException:
    logging.warning("table '%s' already exists", table_id)


def _get_bid_budget_snapshot_dates(
  bigquery_executor: bq_executor.BigQueryExecutor, bq_dataset: str
) -> set[str]:
  try:
    result = bigquery_executor.execute(
      'bid_budgets_snapshots',
      f"""
      SELECT DISTINCT CAST(day AS STRING) AS day
      FROM `{bq_dataset}.bid_budgets_*`
      WHERE
        _TABLE_SUFFIX >= FORMAT_DATE(
           "%Y%m%d",DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY)
        )
      """,
    )
    return set(result['day'])
  except bq_executor.BigQueryExecutorException:
    logging.error('Fail to find bid_budget snapshots')
    return set()


def _get_bid_budget_snapshot_missing_dates(
  snapshot_dates: set[str], date_range: tuple[str, str]
) -> set[str]:
  """Identifies whether snapshots have missing dates.

  Args:
    snapshot_dates: All days when snapshots are present.
    date_range: Period when snapshots should exist.

  Returns:
    Dates when snapshots are missing.
  """
  start_date, end_date = date_range
  dates = [
    date.strftime('%Y-%m-%d')
    for date in pd.date_range(start_date, end_date).to_pydatetime().tolist()
  ]
  return set(dates).difference(snapshot_dates)


def save_restored_change_history(
  bq_writer: bigquery_writer.BigQueryWriter,
  restored_bid_budget_history: pd.DataFrame,
  missing_dates: set[str],
) -> None:
  """Saves snapshot data to BigQuery only for missing dates.

  Args:
    bq_writer: Instantiated BigQueryWriter to perform saving to BQ.
    restored_bid_budget_history: DataFrame with fully restored change history.
    missing_dates: Dates for saving snapshots.
  """
  for date in missing_dates:
    daily_history = restored_bid_budget_history.loc[
      restored_bid_budget_history['day'] == date
    ]
    daily_history.loc[:, ('day')] = datetime.datetime.strptime(
      date, '%Y-%m-%d'
    ).date()
    table_id = f'bid_budgets_{date.replace("-","")}'
    try:
      bq_writer.write(
        gaarf.report.GaarfReport.from_pandas(daily_history), table_id
      )
      logging.info("table '%s' has been created", table_id)
    except (google_api_exceptions.Conflict, ValueError):
      logging.warning("table '%s' already exists", table_id)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--config', dest='gaarf_config', default=None)
  parser.add_argument('--ads-config', dest='ads_config')
  parser.add_argument('--log', '--loglevel', dest='loglevel', default='info')
  parser.add_argument('--logger', dest='logger', default='local')
  parser.add_argument(
    '--restore-bid-budgets', dest='bid_budgets', action='store_true'
  )
  parser.add_argument('--restore-cohorts', dest='cohorts', action='store_true')
  parser.add_argument(
    '--restore-incremental-snapshots', dest='incremental', action='store_true'
  )
  parser.add_argument('--incremental-table', dest='incremental_table')

  args, kwargs = parser.parse_known_args()

  gaarf_utils.init_logging(args.loglevel.upper(), args.logger)
  config = gaarf_utils.ConfigBuilder('gaarf').build(vars(args), kwargs)
  bq_project = config.writer_params.get('project')
  bq_dataset = config.writer_params.get('dataset')
  bigquery_executor = bq_executor.BigQueryExecutor(bq_project)

  if args.incremental:
    get_new_date_for_missing_incremental_snapshots(
      bigquery_executor, bq_dataset, args.incremental_table
    )

  if args.bid_budgets and (
    snapshot_dates := _get_bid_budget_snapshot_dates(
      bigquery_executor, bq_dataset
    )
  ):
    report_fetcher = gaarf.report_fetcher.AdsReportFetcher(
      api_clients.GoogleAdsApiClient(
        path_to_config=args.ads_config, version=config.api_version
      )
    )
    customer_ids = report_fetcher.expand_mcc(
      config.account, config.customer_ids_query
    )
    bq_writer = bigquery_writer.BigQueryWriter(
      project=bq_project,
      dataset=bq_dataset,
      write_disposition='WRITE_EMPTY',
    )
    date_range = (
      (datetime.datetime.now() - datetime.timedelta(days=28)).strftime(
        '%Y-%m-%d'
      ),
      (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        '%Y-%m-%d'
      ),
    )
    if not (
      missing_dates := _get_bid_budget_snapshot_missing_dates(
        snapshot_dates, date_range
      )
    ):
      logging.info('No bid budget snapshots to backfill')
      return
    logging.info(
      'Bid budget snapshots will be backfilled for the following days: %s',
      ', '.join(sorted(missing_dates)),
    )

    restored_bid_budget_history = restore_missing_bid_budgets(
      report_fetcher, customer_ids, date_range
    )
    save_restored_change_history(
      bq_writer, restored_bid_budget_history, missing_dates
    )

  if args.cohorts and (
    snapshot_dates := _get_asset_cohorts_snapshots(
      bigquery_executor, bq_dataset
    )
  ):
    for table_id, query in restore_missing_cohorts(
      snapshot_dates, bq_dataset
    ):
      save_restored_asset_cohort(bigquery_executor, query, table_id)


if __name__ == '__main__':
  main()
