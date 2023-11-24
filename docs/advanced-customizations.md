# How to customize App Reporting Pack data fetching

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
