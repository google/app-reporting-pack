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
"""End-to-end tests for App Reporting Pack.

Tests simulate running application for 3 days which controlling such parameters
as current date.
"""

import datetime
import os
import pathlib
import subprocess

import dotenv
import jinja2
import pandas as pd
import yaml
from gaarf.executors import bq_executor
from gaarf.io import reader

dotenv.load_dotenv()

_CONFIG_PARAMETERS = {
    'account': os.environ.get('ARP_TEST_ACCOUNT'),
    'project': os.environ.get('ARP_TEST_PROJECT'),
}

bigquery_executor = bq_executor.BigQueryExecutor(
    _CONFIG_PARAMETERS.get('project'))


def prepare_configs(configs_path: str, dataset: str) -> None:
  """Injects parameters to config templates."""
  for config in pathlib.Path(configs_path).glob('*.j2'):
    with open(config, mode='r', encoding='utf-8') as f:
      data = f.read()
    template = jinja2.Template(data)
    res = template.render(_CONFIG_PARAMETERS, dataset=dataset)
    new_data = yaml.safe_load(res)
    with open(
        f'{config.parent / config.stem}_prepared.yaml', 'w',
        encoding='utf-8') as f:
      yaml.dump(
          new_data,
          f,
          default_flow_style=False,
          sort_keys=False,
          encoding='utf-8')


def run_app_reporting_pack(configs_path: str):
  for config in sorted(pathlib.Path(configs_path).glob('*_prepared.yaml')):
    command = (f'bash ../../app/run-local.sh -c {config} -q '
               '-g $HOME/google-ads.yaml')
    subprocess.run(command, shell=True, check=False)


def clear_bq_datasets(executor: bq_executor.BigQueryExecutor,
                      bq_dataset_base: str) -> None:
  project_id = executor.project_id
  for dataset_id in (
      bq_dataset_base,
      f'{bq_dataset_base}_output',
      f'{bq_dataset_base}_legacy',
  ):
    executor.client.delete_dataset(
        f'{project_id}.{dataset_id}', delete_contents=True, not_found_ok=True)


def get_bq_data(
    executor: bq_executor.BigQueryExecutor,
    dataset: str,
    validation_query_path: str,
) -> pd.DataFrame:
  validation_query = reader.FileReader().read(validation_query_path)

  return executor.execute(
      'validation',
      validation_query,
      params={
          'macro': {
              'project': executor.project_id,
              'dataset': f'{dataset}_output',
          }
      },
  )


def assert_no_missing_data(
    executor: bq_executor.BigQueryExecutor,
    dataset: str,
    expected_df: pd.DataFrame,
    validation_query_path: str,
):
  results = get_bq_data(executor, dataset, validation_query_path)
  pd.testing.assert_frame_equal(results, expected_df, check_dtype=False)


def test_core_module_with_no_missing_runs():
  configs_path = 'no_missing_runs'
  bq_dataset_base = 'e2e_core_no_missing_runs'
  validation_query_path = 'validation_query_core.sql'

  app_modules = ['core']
  table_suffixes = [
      '20240510',
      '20240518',
      '20240519',
      '20240520',
  ]
  min_days = [
      datetime.date(2024, 5, 10),
      datetime.date(2024, 5, 16),
      datetime.date(2024, 5, 17),
      datetime.date(2024, 5, 18),
  ]
  max_days = [
      datetime.date(2024, 5, 15),
      datetime.date(2024, 5, 16),
      datetime.date(2024, 5, 17),
      datetime.date(2024, 5, 19),
  ]

  expected_df = pd.DataFrame({
      'app_module': app_modules * len(table_suffixes),
      'table_suffix': table_suffixes * len(app_modules),
      'min_day': min_days * len(app_modules),
      'max_day': max_days * len(app_modules),
  })

  clear_bq_datasets(bigquery_executor, bq_dataset_base)
  prepare_configs(configs_path, bq_dataset_base)
  run_app_reporting_pack(configs_path)

  try:
    assert_no_missing_data(bigquery_executor, bq_dataset_base, expected_df,
                           validation_query_path)
  finally:
    clear_bq_datasets(bigquery_executor, bq_dataset_base)


def test_core_module_with_one_missing_run():
  configs_path = 'missing_runs'
  bq_dataset_base = 'e2e_core_missing_runs'
  validation_query_path = 'validation_query_core.sql'

  app_modules = ['core']
  table_suffixes = [
      '20240510',
      '20240519',
      '20240520',
  ]
  min_days = [
      datetime.date(2024, 5, 10),
      datetime.date(2024, 5, 16),
      datetime.date(2024, 5, 18),
  ]
  max_days = [
      datetime.date(2024, 5, 15),
      datetime.date(2024, 5, 17),
      datetime.date(2024, 5, 19),
  ]

  expected_df = pd.DataFrame({
      'app_module': app_modules * len(table_suffixes),
      'table_suffix': table_suffixes * len(app_modules),
      'min_day': min_days * len(app_modules),
      'max_day': max_days * len(app_modules),
  })

  clear_bq_datasets(bigquery_executor, bq_dataset_base)
  prepare_configs(configs_path, bq_dataset_base)
  run_app_reporting_pack(configs_path)

  try:
    assert_no_missing_data(bigquery_executor, bq_dataset_base, expected_df,
                           validation_query_path)
  finally:
    clear_bq_datasets(bigquery_executor, bq_dataset_base)
