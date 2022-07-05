import argparse
import os
import numpy as np
import pandas as pd
from functools import reduce
from google.cloud import bigquery
from gaarf.base_query import BaseQuery
from gaarf.query_executor import AdsReportFetcher


def read_lag_enums_data(lag_enums):
    return pd.read_csv(lag_enums)


def read_api_conversion_lag_data(bq_client, table_name):
    query = f"SELECT * FROM `{table_name}`"
    data = bq_client.query(query).result().to_dataframe()
    return data


def calculate_conversion_lag_cumulative_table(joined_data, group_by):
    grouped = joined_data.groupby(group_by)
    conversion_lag_table = []
    for group_name, df_group in grouped:
        grouped_data = calculate_conversions_by_name_network_lag(df_group)
        cumulative_data = calculate_cumulative_sum_ordered_by_n(
            grouped_data, group_by)
        expanded_data = expand_and_join_lags(cumulative_data)
        incremental_lag = calculate_incremental_lag(expanded_data, group_by)
        conversion_lag_table.append(incremental_lag)
    return reduce(
        lambda left, right: pd.concat([left, right], ignore_index=True),
        conversion_lag_table)




def join_lag_data_and_enums(lag_data, lag_enums):
    return pd.merge(lag_data,
                    lag_enums,
                    on="conversion_lag_bucket",
                    how="left")


def calculate_conversions_by_name_network_lag(joined_data):
    return joined_data.groupby(
        ["network", "conversion_id", "lag_number"], as_index=False).agg({
            "all_conversions":
            np.sum
        }).sort_values(by=["conversion_id", "network", "lag_number"])


def calculate_cumulative_sum_ordered_by_n(grouped_data, base_groupby):
    grouped_data["cumsum"] = grouped_data.groupby(
        base_groupby, as_index=False)["all_conversions"].cumsum()
    total_conversions_by_group = grouped_data.groupby(
        base_groupby, as_index=False)["all_conversions"].sum()
    new_ds = pd.merge(
        grouped_data[base_groupby + ["lag_number", "cumsum"]],
        total_conversions_by_group[base_groupby + ["all_conversions"]],
        on=base_groupby,
        how="left")
    new_ds["pct_conv"] = new_ds["cumsum"] / new_ds["all_conversions"]
    new_ds["shifted_lag_number"] = new_ds["lag_number"].shift(1)
    new_ds["shifted_pct_conv"] = new_ds["pct_conv"].shift(1)
    new_ds[
        "lag_distance"] = new_ds["lag_number"] - new_ds["shifted_lag_number"]
    new_ds["lag_distance"] = new_ds["lag_distance"].fillna(1)
    new_ds["daily_incremental_lag"] = new_ds["pct_conv"] - new_ds[
        "shifted_pct_conv"]
    new_ds["daily_incremental_lag"] = new_ds["daily_incremental_lag"].fillna(
        new_ds["pct_conv"][0])
    return new_ds


def expand_and_join_lags(lag_data):
    return lag_data.loc[lag_data.index.repeat(lag_data.lag_distance)]


def calculate_incremental_lag(expanded_data, base_groupby):
    expanded_data["incremental_lag"] = expanded_data[
        "daily_incremental_lag"] / expanded_data["lag_distance"]
    expanded_data["lag_adjustment"] = expanded_data.groupby(
        base_groupby, as_index=False)["incremental_lag"].cumsum()
    expanded_data["lag_day"] = expanded_data.groupby(
        base_groupby).cumcount() + 1
    return expanded_data[base_groupby + ["lag_day", "lag_adjustment"]]


def write_lag_data(bq_client, cumulative_lag_data, table_id):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_dataframe(
        cumulative_lag_data, table_id, job_config=job_config)
    job.result()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bq.project", dest="project")
    parser.add_argument("--bq.dataset", dest="dataset")
    args = parser.parse_args()

    bq_client = bigquery.Client()
    base_groupby = ["network", "conversion_id"]
    dirname = os.path.dirname(__file__)
    lag_enums = read_lag_enums_data(
        os.path.join(dirname, "conversion_lag_mapping.csv"))
    lag_data = read_api_conversion_lag_data(
        bq_client, f"{args.project}.{args.dataset}.conversion_lag_data")
    joined_data = join_lag_data_and_enums(lag_data, lag_enums)

    conv_lag_table = calculate_conversion_lag_cumulative_table(
        joined_data, base_groupby)
    write_lag_data(
        bq_client, conv_lag_table,
        f"{args.project}.{args.dataset}.conversion_lag_adjustments")

if __name__ == "__main__":
    main()
