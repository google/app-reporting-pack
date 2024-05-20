SELECT
  'core' AS app_module,
  _TABLE_SUFFIX AS table_suffix,
  MIN(day) AS min_day,
  MAX(day) AS max_day
FROM `{project}.{dataset}.ad_group_network_split_*`
GROUP BY 1, 2
UNION ALL
SELECT
  'geo' AS app_module,
  _TABLE_SUFFIX AS table_suffix,
  MIN(day) AS min_day,
  MAX(day) AS max_day
FROM `{project}.{dataset}.geo_performance_*`
GROUP BY 1, 2
UNION ALL
SELECT
  'assets' AS app_module,
  _TABLE_SUFFIX AS table_suffix,
  MIN(day) AS min_day,
  MAX(day) AS max_day
FROM `{project}.{dataset}.asset_performance_*`
GROUP BY 1, 2
UNION ALL
SELECT
  'asset_conversions' AS app_module,
  _TABLE_SUFFIX AS table_suffix,
  MIN(day) AS min_day,
  MAX(day) AS max_day
FROM `{project}.{dataset}.asset_conversion_split_*`
GROUP BY 1, 2
ORDER BY 1, 2;
