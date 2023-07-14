# Storing App Reporting Pack data for extended period

Default behaviour of App Reporting Pack is to overwrite performance data
(`asset_performance`, `ad_group_network_split`, `geo_performance`) only for the
limited window between `start_date` and `end_date` specified during the installation.

During the installation of App Reporting Pack you might want to specify an extended period
to retain performance data.

Let's consider an example:

1. When installing reporting pack you're asked to enter `start_date` (by default last 90 days).
    * This will mean that performance tables will contain only data for the last 90 days
2. If you want to have data for the longer period (i.e. starting from `2023-01-01`) you can specify initial date:
3. During the first data fetching from Google Ads API performance data from `2023-01-01` till yesterday will be fetched.
4. Part of this data (from `2023-01-01` till `start_date`) will be saved to a dedicated table `incremental_asset_performance_20230101`
5. A view `full_asset_performance_view` will be created that contains data for the period between `2023-01-01` and yesterday.

You can use `full_asset_performance_view` in your dashboard or query it separately.
