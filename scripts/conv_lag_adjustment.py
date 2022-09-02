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

from gaarf.api_clients import GoogleAdsApiClient
from gaarf.utils import get_customer_ids
from gaarf.query_executor import AdsReportFetcher
from gaarf.cli.utils import GaarfConfigBuilder

from src.queries import ConversionLagQuery
from src.conv_lag_builder import ConversionLagBuilder


def write_lag_data(bq_client, cumulative_lag_data, table_id):
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", )
    job = bq_client.load_table_from_dataframe(cumulative_lag_data,
                                              table_id,
                                              job_config=job_config)
    job.result()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="gaarf_config", default=None)
    parser.add_argument("--output", dest="save", default="bq")
    parser.add_argument("--ads-config", dest="ads_config")
    parser.add_argument("--account", dest="customer_id")
    parser.add_argument("--api-version", dest="api_version", default="10")
    parser.add_argument("--customer-ids-query",
                        dest="customer_ids_query",
                        default=None)
    parser.add_argument("--customer-ids-query-file",
                        dest="customer_ids_query_file",
                        default=None)

    args = parser.parse_known_args()

    config = GaarfConfigBuilder(args).build()

    project, dataset = config.writer_params.get(
        "project"), config.writer_params.get("dataset")

    google_ads_client = GoogleAdsApiClient(path_to_config=args[0].ads_config,
                                           version=f"v{config.api_version}")
    customer_ids = get_customer_ids(google_ads_client, config.account,
                                    args[0].customer_ids_query)
    report_fetcher = AdsReportFetcher(google_ads_client, customer_ids)

    bq_client = bigquery.Client(project)

    days_ago_180 = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    days_ago_30 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    lag_data = report_fetcher.fetch(
        ConversionLagQuery(days_ago_180, days_ago_30)).to_pandas()

    conv_lag_builder = ConversionLagBuilder(lag_data,
                                            ["network", "conversion_id"])

    conv_lag_table = conv_lag_builder.calculate_reference_values()
    write_lag_data(bq_client, conv_lag_table,
                   f"{project}.{dataset}.conversion_lag_adjustments")


if __name__ == "__main__":
    main()
