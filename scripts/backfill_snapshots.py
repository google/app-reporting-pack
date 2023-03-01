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
from datetime import datetime, timedelta
import itertools
import logging
from rich.logging import RichHandler
from functools import reduce
import pandas as pd
import numpy as np
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from smart_open import open
import yaml

from gaarf.api_clients import GoogleAdsApiClient
from gaarf.utils import get_customer_ids
from gaarf.query_executor import AdsReportFetcher
from gaarf.cli.utils import GaarfConfigBuilder

import src.queries as queries
from src.utils import write_data_to_bq


def restore_history(placeholder_df: pd.DataFrame,
                    partial_history: pd.DataFrame,
                    current_values: pd.DataFrame, name: str) -> pd.DataFrame:
    "Restores dimension history for every date and campaign_id within placeholder_df."
    if not partial_history.empty:
        joined = pd.merge(placeholder_df,
                          partial_history,
                          on=["campaign_id", "day"],
                          how="left")
        joined["filled_backward"] = joined.groupby(
            "campaign_id")["value_old"].bfill()
        joined["filled_forward"] = joined.groupby(
            "campaign_id")["value_new"].ffill()
        joined["filled_forward"] = joined.groupby(
            "campaign_id")["filled_forward"].fillna(
                joined.pop("filled_backward"))
        joined = pd.merge(joined, current_values, on="campaign_id", how="left")
        joined[name] = np.where(joined["filled_forward"].isnull(),
                                joined[name], joined["filled_forward"])
    else:
        joined = pd.merge(placeholder_df,
                          current_values,
                          on="campaign_id",
                          how="left")
    if name != "target_roas":
        joined[name] = joined[name].astype(int)
    return joined[["day", "campaign_id", name]]


def format_partial_change_history(df: pd.DataFrame,
                                  dimension_name: str) -> pd.DataFrame:
    """Performs renaming and selecting last value for the date."""
    df.loc[:, ("day")] = df["change_date"].str.split(expand=True)[0]
    df = df.rename(columns={
        f"old_{dimension_name}": "value_old",
        f"new_{dimension_name}": "value_new"
    })[["day", "campaign_id", "value_old", "value_new"]]
    return df.groupby(["day", "campaign_id"]).last()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="gaarf_config", default=None)
    parser.add_argument("--output", dest="save", default="bq")
    parser.add_argument("--ads-config", dest="ads_config")
    parser.add_argument("--account", dest="customer_id")
    parser.add_argument("--api-version", dest="api_version", default="12")
    parser.add_argument("--log", "--loglevel", dest="loglevel", default="info")
    parser.add_argument("--customer-ids-query",
                        dest="customer_ids_query",
                        default=None)
    parser.add_argument("--customer-ids-query-file",
                        dest="customer_ids_query_file",
                        default=None)

    args = parser.parse_known_args()

    logging.basicConfig(format="%(message)s",
                        level=args[0].loglevel.upper(),
                        datefmt="%Y-%m-%d %H:%M:%S",
                        handlers=[RichHandler(rich_tracebacks=True)])
    logging.getLogger("google.ads.googleads.client").setLevel(logging.WARNING)
    logging.getLogger("smart_open.smart_open_lib").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)

    # Change history can be fetched only for the last 29 days
    days_ago_29 = (datetime.now() - timedelta(days=29)).strftime("%Y-%m-%d")
    days_ago_1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    dates = [
        date.strftime("%Y-%m-%d") for date in pd.date_range(
            days_ago_29, days_ago_1).to_pydatetime().tolist()
    ]
    config = GaarfConfigBuilder(args).build()
    bq_project = config.writer_params.get("project")
    bq_dataset = config.writer_params.get("dataset")

    with open(args[0].ads_config, "r", encoding="utf-8") as f:
        google_ads_config_dict = yaml.safe_load(f)
    google_ads_client = GoogleAdsApiClient(config_dict=google_ads_config_dict,
                                           version=f"v{config.api_version}")
    customer_ids = get_customer_ids(google_ads_client, config.account,
                                    config.customer_ids_query)
    logger.info("Restoring change history for %d accounts: %s",
                len(customer_ids), customer_ids)
    report_fetcher = AdsReportFetcher(google_ads_client, customer_ids)

    # extract change history report
    change_history = report_fetcher.fetch(
        queries.ChangeHistory(days_ago_29, days_ago_1)).to_pandas()

    logger.info("Restoring change history for %d accounts: %s",
                len(customer_ids), customer_ids)
    # Filter rows where budget change occurred
    budgets = change_history.loc[(change_history["old_budget_amount"] > 0)
                                 & (change_history["new_budget_amount"] > 0)]
    if not budgets.empty:
        budgets = format_partial_change_history(budgets, "budget_amount")
        logger.info("%d budgets events were found", len(budgets))
    else:
        logger.info("no budgets events were found")

    # Filter rows where bid change occurred
    target_cpas = change_history.loc[change_history["old_target_cpa"] > 0]
    if not target_cpas.empty:
        target_cpas = format_partial_change_history(target_cpas, "target_cpa")
        logger.info("%d target_cpa events were found", len(target_cpas))
    else:
        logger.info("no target_cpa events were found")

    target_roas = change_history.loc[change_history["old_target_roas"] > 0]
    if not target_roas.empty:
        target_roas = format_partial_change_history(target_roas, "target_roas")
        logger.info("%d target_roas events were found", len(target_roas))
    else:
        logger.info("no target_roas events were found")
    # get all campaigns with an non-zero impressions to build placeholders df
    campaign_ids = report_fetcher.fetch(
        queries.CampaignsWithSpend(days_ago_29, days_ago_1)).to_pandas()
    logger.info("Change history will be restored for %d campaign_ids",
                len(campaign_ids))

    # get latest bids and budgets to replace values in campaigns without any changes
    current_bids_budgets_active_campaigns = report_fetcher.fetch(
        queries.BidsBudgetsActiveCampaigns()).to_pandas()
    current_bids_budgets_inactive_campaigns = report_fetcher.fetch(
        queries.BidsBudgetsInactiveCampaigns(days_ago_29,
                                             days_ago_1)).to_pandas()
    current_bids_budgets = pd.concat([
        current_bids_budgets_inactive_campaigns,
        current_bids_budgets_active_campaigns
    ],
                                     sort=False).drop_duplicates()

    # generate a placeholder dataframe that contain possible variations of
    # campaign_ids with non-zero impressions and last 29 days date range
    placeholders = pd.DataFrame(data=list(
        itertools.product(list(campaign_ids.campaign_id.values), dates)),
                                columns=["campaign_id", "day"])

    restored_budgets = restore_history(placeholders, budgets,
                                       current_bids_budgets, "budget_amount")
    restored_target_cpas = restore_history(placeholders, target_cpas,
                                           current_bids_budgets, "target_cpa")
    restored_target_roas = restore_history(placeholders, target_roas,
                                           current_bids_budgets, "target_roas")
    # Combine restored change histories
    restored_bid_budget_history = reduce(
        lambda left, right: pd.merge(
            left, right, on=["day", "campaign_id"], how="left"),
        [restored_budgets, restored_target_cpas, restored_target_roas])

    # Writer data for each date to BigQuery dated table (with _YYYYMMDD suffix)
    bq_client = bigquery.Client(bq_project)
    for date in dates:
        daily_history = restored_bid_budget_history.loc[
            restored_bid_budget_history["day"] == date]
        daily_history.loc[:, ("day")] = datetime.strptime(date,
                                                          "%Y-%m-%d").date()
        table_id = f"{bq_project}.{bq_dataset}.bid_budgets_{date.replace('-','')}"
        try:
            write_data_to_bq(bq_client=bq_client,
                             data=daily_history,
                             table_id=table_id,
                             write_disposition="WRITE_EMPTY")
            logger.info("table '%s' has been created", table_id)
        except Conflict as e:
            logger.warning("table '%s' already exists", table_id)


if __name__ == "__main__":
    main()
