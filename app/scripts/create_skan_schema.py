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
"""Module responsible for defining iOS SKAN schema.

SKAN Schema can be copied from existing BigQuery table; if no input table is
provided than placeholder table is created.
"""

import argparse
import os

import smart_open
import yaml
from gaarf.cli import utils as gaarf_utils
from gaarf.executors import bq_executor


def update_config(path: str, mode: str) -> None:
  """Helper methods for saving values to config.

  Args:
    path: Config path.
    mode: Skan schema saving mode ('table', 'placeholders').
  """
  if os.path.exists(path):
    with smart_open.open(path, 'r', encoding='utf-8') as f:
      config = yaml.safe_load(f)
  else:
    config = {}
  scripts_config = {'scripts': {'skan_mode': {'mode': mode}}}
  if config.get('scripts'):
    config.get('scripts').update(scripts_config.get('scripts'))
  else:
    config.update(scripts_config)
  with smart_open.open(path, 'w', encoding='utf-8') as f:
    yaml.dump(
      config, f, default_flow_style=False, sort_keys=False, encoding='utf-8'
    )


def has_existing_schema(
  bigquery_executor: bq_executor.BigQueryExecutor, bq_dataset: str
) -> bool:
  """Checks whether SKAN schema has been already copied to BigQuery.

  Args:
    bigquery_executor: Executor responsible for writing data to BigQuery.
    bq_dataset: BigQuery dataset to write data to.
  """
  try:
    bigquery_executor.execute(
      'check_existing_skan_schema',
      f'SELECT app_id FROM `{bq_dataset}.skan_schema_input_table` LIMIT 0',
    )
    return True
  except bq_executor.BigQueryExecutorException:
    return False


def copy_schema(
  bigquery_executor: bq_executor.BigQueryExecutor,
  config: gaarf_utils.GaarfBqConfig,
) -> None:
  """Copies SKAN schema existing table in BigQuery.

  Args:
    bigquery_executor: Executor responsible for writing data to BigQuery.
    config: GaarfBqConfig with parameters for copying.
  """
  query = """
            CREATE OR REPLACE TABLE `{bq_dataset}.skan_schema_input_table` AS
            SELECT
              app_id,
              skan_conversion_value,
              skan_event_count,
              skan_event_value_low,
              skan_event_value_high,
              skan_event_value_mean,
              skan_mapped_event
            FROM `{skan_schema_input_table}`;

        """
  bigquery_executor.execute('skan_schema', query, config.params)


def generate_placeholder_schema(
  bigquery_executor: bq_executor.BigQueryExecutor, bq_dataset: str
) -> None:
  """Creates table with empty values for SKAN schema.

  Args:
    bigquery_executor: Executor responsible for writing data to BigQuery.
    bq_dataset: BigQuery dataset to write data to.
  """
  query = f"""
            CREATE OR REPLACE TABLE `{bq_dataset}.skan_schema_input_table` AS
            SELECT
              "com.example" AS app_id,
              0 AS skan_conversion_value,
              0 AS skan_event_count,
              0.1 AS skan_event_value_low,
              0.1 AS skan_event_value_high,
              0.1 AS skan_event_value_mean,
              "" AS skan_mapped_event
            LIMIT 1
        """
  bigquery_executor.execute('skan_schema', query)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--config', dest='gaarf_config', default=None)
  parser.add_argument('-m', '--mode', dest='mode', default='placeholders')
  parser.add_argument('--log', '--loglevel', dest='loglevel', default='info')
  parser.add_argument('--logger', dest='logger', default='local')
  parser.add_argument('--save-config', dest='save_config', action='store_true')
  parser.add_argument('--dry-run', dest='dry_run', action='store_true')
  parser.set_defaults(save_config=False)
  parser.set_defaults(dry_run=False)
  args, kwargs = parser.parse_known_args()

  logger = gaarf_utils.init_logging(
    loglevel=args.loglevel.upper(), logger_type=args.logger
  )

  config = gaarf_utils.ConfigBuilder('gaarf-bq').build(vars(args), kwargs)
  bq_dataset = config.params.get('macros', {}).get('bq_dataset')
  bigquery_executor = bq_executor.BigQueryExecutor(config.project)
  if gaarf_config := args.gaarf_config:
    with smart_open.open(gaarf_config, 'r', encoding='utf-8') as f:
      raw_config = yaml.safe_load(f)
    mode = (
      raw_config.get('scripts', {}).get('skan_mode', {}).get('mode')
      or args.mode
    )
    logger.info('Saving SKAN mode to the config')
    if args.save_config:
      update_config(path=gaarf_config, mode=mode)
  else:
    mode = args.mode

  if args.dry_run:
    exit()
  if mode == 'placeholders' or not config.params.get('macro', {}).get(
    'skan_schema_input_table'
  ):
    logger.info('Generating placeholders for SKAN schema')
    generate_placeholder_schema(bigquery_executor, bq_dataset)
  else:
    try:
      logger.info('Copying SKAN schema')
      copy_schema(bigquery_executor, config)
    except bq_executor.BigQueryExecutorException:
      if not has_existing_schema(bigquery_executor, config):
        logger.info(
          'Failed to copy SKAN schema, generating placeholders instead'
        )
        generate_placeholder_schema(bigquery_executor, bq_dataset)
      else:
        logger.info('Failed to copy SKAN schema, re-using existing schema')


if __name__ == '__main__':
  main()
