# Storing App Reporting Pack data for extended period

Default behaviour of App Reporting Pack is to overwrite performance data
(`asset_performance`, `asset_conversion_split`, `ad_group_network_split`,
`geo_performance`, `ios_decoder`) only for the limited reporting window
(i.e. last 90 days) specified during the installation.

During the installation of App Reporting Pack you might want to specify an
extended period to retain performance data.

Let's consider an example (suppose we're running the solution on `2023-10-26`):

1. When installing reporting pack you're asked to enter `reporting_window` (by default last 90 days).
    * This will mean that performance tables will contain only data for the last 90 days
2. If you want to have data for the longer period (i.e. starting from `2023-01-01`) you can specify initial date.
3. During the first data fetching from Google Ads API performance data from `2023-01-01` till yesterday will be fetched.
4. Part of this data (from `2023-01-01` till `current_date` minus `reporting_window`) will be saved to a table `asset_performance_20230101`
5. Part of this data (from `current_date` minus `reporting_window` till `yesterday`) will be saved to a table `asset_performance_20231026`
5. You can get the full data by queuing `asset_performance_*`  table
6. During the next run of the solution (tomorrow) we'll fetch data only for the last 90 days,
save the first day (`current_date` minus `reporting_window`) as `asset_performance_20231026`
and the rest as `asset_performance_20231027`.
7. The full data (starting from `2023-01-01`) is still available as `asset_performance_*` table.
