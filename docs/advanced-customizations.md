# How to customize App Reporting Pack data fetching

## Backfill performance data

If already set up a solution and want your performance tables (`asset_performance`,
`asset_converion_split`, `ad_group_network_split`, `ios_skan_decoder`,
`geo_performance`) to contains historical data from a certain date in the past,
please do the following:

* In `app_reporting_pack.yaml` under `gaarf > params > macro`
add `initial_load_date: 'YYYY-MM-DD'`, where `YYYY-MM-DD` is the first date
you want to have performance data loaded.

```
gaarf:
  params:
    macro:
      initial_load_date: '2022-01-01'
      start_date: :YYYYMMDD-30
      end_date: :YYYYMMDD-1
```

## Providing custom conversion mapping

If you want your tables (`asset_performance` and `ad_group_network_split`)
to contain custom conversion mapping (i.e. column that contains conversion(s)
only with a particular conversion name) you can add the following lines to
your `app_reporting_pack.yaml` in "conversion_alias: Conversion Name" format:

```
template:
  custom_conversions:
    - conversion_name_1: "Your Conv','Second Conv"
      conversion_name_2: "My Conv"
```

This will the following columns to your tables in BigQuery:

* `conversions_conversion_name_1`
* `conversions_value_conversion_name_1`
* `conversions_conversion_name_2`
* `conversions_value_conversion_name_2`

You can add one or more conversion names to a given conversion alias; in case
of several conversion they should be separated with `','`.
