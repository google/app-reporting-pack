# How to specify iOS SKAN schema

App Reporting Pack SKAN Reports can be enriched with decoded SKAN conversion values by
having a table in BigQuery with the following iOS SKAN schema fields:

* `app_id`
* `skan_conversion_value`
* `skan_event_count`
* `skan_event_value_low`
* `skan_event_value_high`
* `skan_event_value_mean`
* `skan_mapped_event`

You can follow an official documentation on [uploading data](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#loading_csv_data_into_a_table) to BigQuery.
