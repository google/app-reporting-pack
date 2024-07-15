-- Copyright 2024 Google LLC
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

-- Generates common views to be used in the postprocessing scripts.
--
-- @param {bq_dataset} Dataset in BigQuery.

-- App campaign settings for creating output table
CREATE OR REPLACE VIEW `{bq_dataset}.AppCampaignSettingsView`
AS (
  WITH
    RawConversionIds AS (
      SELECT
        ACS.campaign_id,
        ACS.campaign_sub_type,
        ACS.app_id,
        ACS.app_store,
        CASE
          WHEN ACS.bidding_strategy = 'OPTIMIZE_INSTALLS_TARGET_INSTALL_COST'
            THEN 'Installs'
          WHEN
            ACS.bidding_strategy
            = 'OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST'
            THEN 'Installs Advanced'
          WHEN
            ACS.bidding_strategy
            = 'OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST'
            THEN 'Actions'
          WHEN
            ACS.bidding_strategy
            = 'OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST'
            THEN 'Maximize Conversions'
          WHEN ACS.bidding_strategy = 'OPTIMIZE_RETURN_ON_ADVERTISING_SPEND'
            THEN 'Target ROAS'
          WHEN
            ACS.bidding_strategy
            = 'OPTIMIZE_PRE_REGISTRATION_CONVERSION_VOLUME'
            THEN 'Preregistrations'
          ELSE 'Unknown'
          END AS bidding_strategy,
        ACS.start_date,
        M.conversion_id,
        M.conversion_source,
        M.conversion_type,
        M.conversion_name
      FROM
        `{bq_dataset}.app_campaign_settings` AS ACS,
        UNNEST(SPLIT(ACS.target_conversions, '|')) AS conversion_ids
      LEFT JOIN `{bq_dataset}.app_conversions_mapping` AS M
        ON
          SPLIT(conversion_ids, '/')[SAFE_OFFSET(3)]
          = CAST(M.conversion_id AS STRING)
      GROUP BY ALL
    ),
    InstallConversionId AS (
      SELECT
        campaign_id,
        STRING_AGG(CAST(conversion_id AS STRING)) AS install_conversion_id,
        ARRAY_TO_STRING(
          ARRAY_AGG(DISTINCT conversion_name ORDER BY conversion_name), ' | ')
          AS install_target_conversions,
        ARRAY_TO_STRING(
          ARRAY_AGG(DISTINCT conversion_source ORDER BY conversion_source),
          ' | ') AS install_conversion_sources,
        COUNT(DISTINCT conversion_id) AS n_of_install_target_conversions
      FROM RawConversionIds
      WHERE conversion_type = 'DOWNLOAD'
      GROUP BY 1
    ),
    InappConversionIds AS (
      SELECT
        campaign_id,
        STRING_AGG(CAST(conversion_id AS STRING)) AS inapp_conversion_ids,
        ARRAY_TO_STRING(
          ARRAY_AGG(DISTINCT conversion_name ORDER BY conversion_name), ' | ')
          AS inapp_target_conversions,
        ARRAY_TO_STRING(
          ARRAY_AGG(DISTINCT conversion_source ORDER BY conversion_source),
          ' | ') AS inapp_conversion_sources,
        COUNT(DISTINCT conversion_id) AS n_of_inapp_target_conversions
      FROM RawConversionIds
      WHERE conversion_type != 'DOWNLOAD'
      GROUP BY 1
    )
  SELECT
    R.campaign_id,
    ANY_VALUE(R.campaign_sub_type) AS campaign_sub_type,
    ANY_VALUE(R.app_id) AS app_id,
    ANY_VALUE(R.app_store) AS app_store,
    ANY_VALUE(R.bidding_strategy) AS bidding_strategy,
    ANY_VALUE(R.start_date) AS start_date,
    ANY_VALUE(I.install_conversion_id) AS install_converion_id,
    ANY_VALUE(A.inapp_conversion_ids) AS inapp_conversion_ids,
    COALESCE(
      ANY_VALUE(A.inapp_target_conversions),
      ANY_VALUE(I.install_target_conversions)) AS target_conversions,
    COALESCE(
      ANY_VALUE(A.inapp_conversion_sources),
      ANY_VALUE(I.install_conversion_sources)) AS conversion_sources,
    COALESCE(
      ANY_VALUE(A.n_of_inapp_target_conversions),
      ANY_VALUE(I.n_of_install_target_conversions)) AS n_of_target_conversions
  FROM RawConversionIds AS R
  LEFT JOIN InstallConversionId AS I
    USING (campaign_id)
  LEFT JOIN InappConversionIds AS A
    USING (campaign_id)
  GROUP BY 1
);

-- Campaign level geo and language targeting
CREATE OR REPLACE VIEW `{bq_dataset}.GeoLanguageView`
AS (
  SELECT
    COALESCE(CGT.campaign_id, L.campaign_id)
      AS campaign_id,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT GT.country_code ORDER BY country_code), ' | ') AS geos,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT L.language ORDER BY language), ' | ')
      AS languages,
  FROM `{bq_dataset}.campaign_geo_targets` AS CGT
  LEFT JOIN `{bq_dataset}.geo_target_constant` AS GT
    ON
      CAST(CGT.geo_target AS STRING)
      = CAST(GT.constant_id AS STRING)
  FULL JOIN `{bq_dataset}.campaign_languages` AS L
    ON CGT.campaign_id = L.campaign_id
  GROUP BY 1
);

-- Conversion Lag adjustment placeholder data
CREATE OR REPLACE VIEW `{bq_dataset}.ConversionLagAdjustments`
AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL lag_day DAY) AS adjustment_date,
    network,
    conversion_id,
    lag_adjustment
  FROM `{bq_dataset}.conversion_lag_adjustments`
);
