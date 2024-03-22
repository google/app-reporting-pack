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
"""Entrypoint for performing various backfilling operations."""

import argparse
from datetime import datetime, timedelta
from bisect import bisect_left
import itertools
import logging
from functools import reduce
import pandas as pd
import numpy as np
from google.api_core.exceptions import Conflict, NotFound, BadRequest
from google.cloud import bigquery
from smart_open import open
import yaml

from gaarf.api_clients import GoogleAdsApiClient
from gaarf.query_executor import AdsReportFetcher
from gaarf.cli.utils import GaarfConfig, GaarfConfigBuilder, init_logging

import src.queries as queries
from src.utils import write_data_to_bq


def get_new_date_for_missing_incremental_snapshots(bq_client: bigquery.Client,
                                                   bq_dataset: str,
                                                   table_name: str) -> None:
    """Checks for missing snapshots and outputs new start date.

    If there's a gap in data we need to remove the table with the problematic
    TABLE_SUFFIX and find a new start_date that should be use by the
    application when running data fetching.

    Args:
        bq_client: Client for BigQuery I/O.
        bq_dataset: BQ dataset to get data from.
        table_name: BQ table that are checked for incremental snapshots.
    """
    start_date = ""
    base_table_id = f"{bq_client.project}.{bq_dataset}.{table_name}"
    try:
        job = bq_client.query("SELECT TABLE_SUFFIX, new_start_date "
                              f"FROM `{base_table_id}_missing`")
        result = job.result().to_dataframe()
        if not result.empty:
            start_date = result.new_start_date.squeeze().strftime(
                "%Y-%m-%d")
            suffix = result.TABLE_SUFFIX.squeeze()
            delete_ddl = f"DELETE `{base_table_id}_{suffix}`;"
            job = bq_client.query(delete_ddl)
            job.result()
    except (BadRequest, NotFound):
        pass
    print(start_date)


def restore_missing_bid_budgets(google_ads_client: GoogleAdsApiClient,
                                config: GaarfConfig,
                                bq_client: bigquery.Client,
                                bq_dataset: str) -> None:
    try:
        job = bq_client.query(f"SELECT DISTINCT CAST(day AS STRING) AS day "
                              f"FROM `{bq_dataset}.bid_budgets_*`")
        result = job.result()
    except BadRequest:
        return
    snapshot_dates = set(result.to_dataframe()["day"])

    # Change history can be fetched only for the last 28 days
    days_ago_28 = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")
    days_ago_1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    dates = [
        date.strftime("%Y-%m-%d") for date in pd.date_range(
            days_ago_28, days_ago_1).to_pydatetime().tolist()
    ]
    missing_dates = set(dates).difference(snapshot_dates)
    if not missing_dates:
        logging.info("No bid budget snapshots to backfill")
    report_fetcher = AdsReportFetcher(google_ads_client)
    customer_ids = report_fetcher.expand_mcc(config.account,
                                             config.customer_ids_query)

    logging.info("Restoring change history for %d accounts: %s",
                 len(customer_ids), customer_ids)
    # extract change history report
    change_history = report_fetcher.fetch(
        queries.ChangeHistory(days_ago_28, days_ago_1),
        customer_ids).to_pandas()

    # Filter rows where budget change occurred
    budgets = change_history.loc[(change_history["old_budget_amount"] > 0)
                                 & (change_history["new_budget_amount"] > 0)]
    if not budgets.empty:
        budgets = format_partial_change_history(budgets, "budget_amount")
        logging.info("%d budgets events were found", len(budgets))
    else:
        logging.info("no budgets events were found")

    # Filter rows where bid change occurred
    target_cpas = change_history.loc[change_history["old_target_cpa"] > 0]
    if not target_cpas.empty:
        target_cpas = format_partial_change_history(target_cpas, "target_cpa")
        logging.info("%d target_cpa events were found", len(target_cpas))
    else:
        logging.info("no target_cpa events were found")

    target_roas = change_history.loc[change_history["old_target_roas"] > 0]
    if not target_roas.empty:
        target_roas = format_partial_change_history(target_roas, "target_roas")
        logging.info("%d target_roas events were found", len(target_roas))
    else:
        logging.info("no target_roas events were found")
    # get all campaigns with an non-zero impressions to build placeholders df
    campaign_ids = report_fetcher.fetch(
        queries.CampaignsWithSpend(days_ago_28, days_ago_1),
        customer_ids).to_pandas()
    logging.info("Change history will be restored for %d campaign_ids",
                 len(campaign_ids))

    # get latest bids and budgets to replace values in campaigns without any changes
    current_bids_budgets_active_campaigns = report_fetcher.fetch(
        queries.BidsBudgetsActiveCampaigns(), customer_ids).to_pandas()
    current_bids_budgets_inactive_campaigns = report_fetcher.fetch(
        queries.BidsBudgetsInactiveCampaigns(days_ago_28, days_ago_1),
        customer_ids).to_pandas()
    current_bids_budgets = pd.concat([
        current_bids_budgets_inactive_campaigns,
        current_bids_budgets_active_campaigns
    ],
                                     sort=False).drop_duplicates()

    # generate a placeholder dataframe that contain possible variations of
    # campaign_ids with non-zero impressions and last 28 days date range
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
    for date in missing_dates:
        daily_history = restored_bid_budget_history.loc[
            restored_bid_budget_history["day"] == date]
        daily_history.loc[:, ("day")] = datetime.strptime(date,
                                                          "%Y-%m-%d").date()
        table_id = f"{bq_dataset}.bid_budgets_{date.replace('-','')}"
        try:
            write_data_to_bq(bq_client=bq_client,
                             data=daily_history,
                             table_id=table_id,
                             write_disposition="WRITE_EMPTY")
            logging.info("table '%s' has been created", table_id)
        except Conflict as e:
            logging.warning("table '%s' already exists", table_id)


def restore_missing_cohorts(bq_client: bigquery.Client,
                            bq_dataset: str) -> None:
    try:
        job = bq_client.query(f"SELECT DISTINCT _TABLE_SUFFIX AS day FROM "
                              f"`{bq_dataset}.conversion_lags_*` ORDER BY 1")
        result = job.result()
    except BadRequest:
        return
    snapshot_dates = set(result.to_dataframe()["day"])
    dates = [
        date for date in pd.date_range(min(snapshot_dates), max(
            snapshot_dates)).to_pydatetime().tolist()
    ]
    missing_dates = list(set(dates).difference(snapshot_dates))
    snapshot_dates = sorted(
        [datetime.strptime(date, "%Y%m%d").date() for date in snapshot_dates])
    if not missing_dates:
        logging.info("No asset cohort snapshots to backfill")
        return
    for date in sorted(missing_dates):
        index = bisect_left(snapshot_dates, date.date())
        if index == 0:
            continue
        last_available_date = snapshot_dates[index - 1]
        date_diff = (date.date() - last_available_date).days
        date = date.strftime("%Y%m%d")
        table_id = f"{bq_dataset}.conversion_lags_{date}"
        query = f"""
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
            FROM `{bq_dataset}.conversion_lags_{last_available_date.strftime("%Y%m%d")}`
            """
        job = bq_client.query(query)
        try:
            result = job.result()
            logging.info("table '%s' has been created", table_id)
        except Conflict as e:
            logging.warning("table '%s' already exists", table_id)


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
    parser.add_argument("--api-version", dest="api_version", default="14")
    parser.add_argument("--log", "--loglevel", dest="loglevel", default="info")
    parser.add_argument("--logger", dest="logger", default="local")
    parser.add_argument("--restore-bid-budgets",
                        dest="bid_budgets",
                        action="store_true")
    parser.add_argument("--restore-cohorts",
                        dest="cohorts",
                        action="store_true")
    parser.add_argument("--restore-incremental-snapshots",
                        dest="incremental",
                        action="store_true")
    parser.add_argument("--incremental-table",
                        dest="incremental_table")
    parser.add_argument("--customer-ids-query",
                        dest="customer_ids_query",
                        default=None)
    parser.add_argument("--customer-ids-query-file",
                        dest="customer_ids_query_file",
                        default=None)

    args = parser.parse_known_args()

    logger = init_logging(loglevel=args[0].loglevel.upper(),
                          logger_type=args[0].logger)

    config = GaarfConfigBuilder(args).build()
    bq_project = config.writer_params.get("project")
    bq_dataset = config.writer_params.get("dataset")
    bq_client = bigquery.Client(bq_project)
    with open(args[0].ads_config, "r", encoding="utf-8") as f:
        google_ads_config_dict = yaml.safe_load(f)
    google_ads_client = GoogleAdsApiClient(config_dict=google_ads_config_dict,
                                           version=f"v{config.api_version}")
    if args[0].incremental:
        get_new_date_for_missing_incremental_snapshots(
            bq_client, bq_dataset, args[0].incremental_table)
    if args[0].bid_budgets:
        restore_missing_bid_budgets(google_ads_client, config, bq_client,
                                    bq_dataset)
    if args[0].cohorts:
        restore_missing_cohorts(bq_client, bq_dataset)


if __name__ == "__main__":
    main()
