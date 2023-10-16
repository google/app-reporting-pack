-- Copyright 2023 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

--TODO: Create materialized view
CREATE OR REPLACE VIEW `{bq_dataset}.AssetCohorts` AS (
WITH
    RawAssetConversionLags AS (
      SELECT * FROM `{bq_dataset}.conversion_lags_*`
    ),
    DistinctAdGroupAssetDimensions AS (
      SELECT DISTINCT
        day_of_interaction,
        ad_group_id,
        asset_id,
        network,
        field_type
      FROM RawAssetConversionLags
    ),
    Final AS (
      SELECT
        day_of_interaction,
        ad_group_id,
        asset_id,
        network,
        field_type,
        lag,
        installs AS installs_,
        inapps AS inapps_,
        view_through_conversions AS view_through_conversions_,
        conversions_value AS conversions_value_,
        LAST_VALUE(installs IGNORE NULLS) OVER (
            PARTITION BY day_of_interaction, ad_group_id, asset_id, network, field_type
            ORDER BY lag
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS installs,
        LAST_VALUE(inapps IGNORE NULLS) OVER (
            PARTITION BY day_of_interaction, ad_group_id, asset_id, network, field_type
            ORDER BY lag
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS inapps,
        LAST_VALUE(view_through_conversions IGNORE NULLS) OVER (
            PARTITION BY day_of_interaction, ad_group_id, asset_id, network, field_type
            ORDER BY lag
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS view_through_conversions,
        LAST_VALUE(conversions_value IGNORE NULLS) OVER (
            PARTITION BY day_of_interaction, ad_group_id, asset_id, network, field_type
            ORDER BY lag
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS conversions_value
      FROM DistinctAdGroupAssetDimensions,
          UNNEST(GENERATE_ARRAY(1, 90)) AS lag
      LEFT JOIN RawAssetConversionLags
          USING(day_of_interaction, ad_group_id, asset_id, network, lag, field_type)
      -- Filter only lags in the past
      WHERE day_of_interaction <= DATE_SUB(CURRENT_DATE(), INTERVAL lag DAY)
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )
SELECT
  day_of_interaction,
  ad_group_id,
  asset_id,
  field_type,
  network,
  STRUCT(
    ARRAY_AGG(lag ORDER BY lag) AS lags,
    ARRAY_AGG(installs ORDER BY lag) AS installs,
    ARRAY_AGG(inapps ORDER BY lag) AS inapps,
    ARRAY_AGG(conversions_value ORDER BY lag) AS conversions_value,
    ARRAY_AGG(view_through_conversions ORDER BY lag) AS view_through_conversions
  ) AS lag_data
FROM Final
WHERE
  day_of_interaction IS NOT NULL
  AND installs IS NOT NULL
  AND inapps IS NOT NULL
  AND conversions_value IS NOT NULL
  AND view_through_conversions IS NOT NULL
  AND lag <= 90
GROUP BY 1, 2, 3, 4, 5
);
