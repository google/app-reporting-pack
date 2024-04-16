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

from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, asdict
import argparse
import os
import logging
import google
from rich.logging import RichHandler
import yaml
from smart_open import open

from gaarf.bq_executor import BigQueryExecutor
from gaarf.cli.utils import GaarfBqConfigBuilder, GaarfBqConfig, init_logging


def update_config(path: str, mode: str) -> None:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    else:
        config = {}
    scripts_config = {"scripts": {"skan_mode": {"mode": mode}}}
    if config.get("scripts"):
        config.get("scripts").update(scripts_config.get("scripts"))
    else:
        config.update(scripts_config)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(config,
                  f,
                  default_flow_style=False,
                  sort_keys=False,
                  encoding="utf-8")


def has_existing_schema(bq_executor, config: GaarfBqConfig) -> bool:
    try:
        schema ="{bq_dataset}.skan_schema".format(**config.params.get("macro"))
        bq_executor.client.get_table(schema)
        return True
    except Exception:
        return False


def copy_schema(bq_executor, config: GaarfBqConfig) -> None:
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
    bq_executor.execute("skan_schema", query, config.params)


def generate_placeholder_schema(bq_executor, config: GaarfBqConfig) -> None:
    query = """
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
    bq_executor.execute("skan_schema", query, config.params)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="gaarf_config", default=None)
    parser.add_argument("-m", "--mode", dest="mode", default="placeholders")
    parser.add_argument("--log", "--loglevel", dest="loglevel", default="info")
    parser.add_argument("--logger", dest="logger", default="local")
    parser.add_argument("--save-config",
                        dest="save_config",
                        action="store_true")
    parser.add_argument("--config-destination",
                        dest="save_config_dest",
                        default="config.yaml")

    parser.add_argument("--project", dest="project", default=None)
    parser.add_argument("--dataset-location", dest="dataset_location", default=None)
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.set_defaults(save_config=False)
    parser.set_defaults(dry_run=False)
    args = parser.parse_known_args()

    save_config = args[0].save_config
    dry_run = args[0].dry_run

    logger = init_logging(loglevel=args[0].loglevel.upper(),
                          logger_type=args[0].logger)

    config = GaarfBqConfigBuilder(args).build()
    bq_executor = BigQueryExecutor(config.project)
    if gaarf_config := args[0].gaarf_config:
        with open(gaarf_config, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)
        mode = raw_config.get("scripts", {}).get("skan_mode",
                                                 {}).get("mode") or args[0].mode
        if save_config:
            update_config(path=gaarf_config, mode=mode)

    else:
        mode = args[0].mode
    if dry_run:
        logger.info("Saving SKAN mode to the config")
        exit()
    if (mode == "placeholders" or
            not config.params.get("macro", {}).get("skan_schema_input_table")):
        logger.info("Generating placeholders for SKAN schema")
        generate_placeholder_schema(bq_executor, config)
        exit()
    try:
        logger.info("Copying SKAN schema")
        copy_schema(bq_executor, config)
    except Exception:
        if not has_existing_schema(bq_executor, config):
            logger.info(
            "Failed to copy SKAN schema, generating placeholders instead")
            generate_placeholder_schema(bq_executor, config)
        else:
            logger.info(
            "Failed to copy SKAN schema, re-using existing schema")


if __name__ == "__main__":
    main()
