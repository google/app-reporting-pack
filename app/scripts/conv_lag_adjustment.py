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
"""Calculates values of conversion lags for a given day of lag.

Script creates table in BigQuery that contains adjustment coefficients for
each lag (from 1 till 90 day) for a given conversion_id and network
"""

import argparse
from datetime import datetime, timedelta

import gaarf
from gaarf import api_clients
from gaarf.cli import utils as gaarf_utils
from gaarf.io.writers import bigquery_writer
from src import conv_lag_builder, queries


def generate_placeholders() -> gaarf.report.GaarfReport:
  """Returns empty report if no lag data were found."""
  return gaarf.report.GaarfReport(
    results=[['', '', 0, 0.0]],
    column_names=['network', 'conversion_id', 'lag_day', 'lag_adjustment'],
  )


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--config', dest='gaarf_config', default=None)
  parser.add_argument('--ads-config', dest='ads_config')
  parser.add_argument('--log', '--loglevel', dest='loglevel', default='info')
  parser.add_argument('--logger', dest='logger', default='local')

  args, kwargs = parser.parse_known_args()

  config = gaarf_utils.ConfigBuilder('gaarf').build(vars(args), kwargs)

  project, dataset = (
    config.writer_params.get('project'),
    config.writer_params.get('dataset'),
  )

  report_fetcher = gaarf.report_fetcher.AdsReportFetcher(
    api_clients.GoogleAdsApiClient(
      path_to_config=args.ads_config, version=config.api_version
    )
  )
  customer_ids = report_fetcher.expand_mcc(
    config.account, config.customer_ids_query
  )

  days_ago_180 = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
  days_ago_30 = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

  lag_data = report_fetcher.fetch(
    queries.ConversionLagQuery(days_ago_180, days_ago_30), customer_ids
  )
  if lag_data:
    conv_lag_table = conv_lag_builder.ConversionLagBuilder(
      lag_data.to_pandas(), ['network', 'conversion_id']
    ).calculate_reference_values()
  else:
    conv_lag_table = generate_placeholders()
  bigquery_writer.BigQueryWriter(
    project=project,
    dataset=dataset,
  ).write(conv_lag_table, 'conversion_lag_adjustments')


if __name__ == '__main__':
  main()
