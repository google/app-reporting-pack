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

import argparse
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
from smart_open import open
import yaml

from gaarf.api_clients import GoogleAdsApiClient
from gaarf.query_executor import AdsReportFetcher
from gaarf.cli import utils as gaarf_utils

from src.queries import ConversionLagQuery
from src.conv_lag_builder import ConversionLagBuilder
from src.utils import write_data_to_bq


def generate_placeholders():
    df = pd.DataFrame(
        data=[["", "", 0, 0.0]],
        columns=["network", "conversion_id", "lag_day", "lag_adjustment"])
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="gaarf_config", default=None)
    parser.add_argument("--output", dest="save", default="bq")
    parser.add_argument("--ads-config", dest="ads_config")
    parser.add_argument("--account", dest="customer_id")
    parser.add_argument("--api-version", dest="api_version", default="12")
    parser.add_argument("--customer-ids-query",
                        dest="customer_ids_query",
                        default=None)
    parser.add_argument("--customer-ids-query-file",
                        dest="customer_ids_query_file",
                        default=None)
    parser.add_argument("--log", "--loglevel", dest="loglevel", default="info")
    parser.add_argument("--logger", dest="logger", default="local")

    args = parser.parse_known_args()

    config = gaarf_utils.ConfigBuilder('gaarf').build(vars(args[0]), args[1])

    project, dataset = config.writer_params.get(
        "project"), config.writer_params.get("dataset")

    with open(args[0].ads_config, "r", encoding="utf-8") as f:
        google_ads_config_dict = yaml.safe_load(f)
    google_ads_client = GoogleAdsApiClient(config_dict=google_ads_config_dict,
                                           version=f"v{config.api_version}")
    report_fetcher = AdsReportFetcher(google_ads_client)
    customer_ids = report_fetcher.expand_mcc(config.account,
                                             args[0].customer_ids_query)

    bq_client = bigquery.Client(project)

    days_ago_180 = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    days_ago_30 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    lag_data = report_fetcher.fetch(
        ConversionLagQuery(days_ago_180, days_ago_30), customer_ids)
    if lag_data:
        conv_lag_builder = ConversionLagBuilder(lag_data.to_pandas(),
                                                ["network", "conversion_id"])

        conv_lag_table = conv_lag_builder.calculate_reference_values()
    else:
        conv_lag_table = generate_placeholders()
    write_data_to_bq(bq_client, conv_lag_table,
                     f"{project}.{dataset}.conversion_lag_adjustments")


if __name__ == "__main__":
    main()
