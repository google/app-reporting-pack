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

-- Represents basic app campaign settings that are used when creating
-- final tables for dashboards
CREATE OR REPLACE VIEW `{bq_dataset}.AppCampaignSettingsView`
AS (
    WITH RawConversionIds AS (
        SELECT
        ACS.campaign_id,
        ACS.campaign_sub_type,
        ACS.app_id,
        ACS.app_store,
        CASE
            WHEN ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST" THEN "Installs"
            WHEN ACS.bidding_strategy = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST" THEN "Installs Advanced"
            WHEN ACS.bidding_strategy = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST" THEN "Actions"
            WHEN ACS.bidding_strategy = "OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST" THEN "Maximize Conversions"
            WHEN ACS.bidding_strategy = "OPTIMIZE_RETURN_ON_ADVERTISING_SPEND" THEN "Target ROAS"
            WHEN ACS.bidding_strategy = "OPTIMIZE_PRE_REGISTRATION_CONVERSION_VOLUME" THEN "Preregistrations"
            ELSE "Unknown"
            END AS bidding_strategy,
        ACS.start_date,
        conversion_id,
        conversion_source,
        conversion_type,
        conversion_name
    FROM {bq_dataset}.app_campaign_settings AS ACS,
    UNNEST(SPLIT(ACS.target_conversions, "|")) AS conversion_ids
    LEFT JOIN {bq_dataset}.app_conversions_mapping AS Mapping
        ON conversion_ids = CAST(Mapping.conversion_id AS STRING)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    InstallConversionId AS (
        SELECT
            campaign_id,
            STRING_AGG(conversion_id) AS install_conversion_id,
            ARRAY_TO_STRING(ARRAY_AGG(DISTINCT conversion_name ORDER BY conversion_name), " | ") AS install_target_conversions,
            ARRAY_TO_STRING(ARRAY_AGG(DISTINCT conversion_source ORDER BY conversion_source), " | ") AS install_conversion_sources,
            COUNT(DISTINCT conversion_id) AS n_of_install_target_conversions
        FROM RawConversionIds
        WHERE conversion_type = "DOWNLOAD"
        GROUP BY 1
    ),
    InappConversionIds AS (
        SELECT
            campaign_id,
            STRING_AGG(conversion_id) AS inapp_conversion_ids,
            ARRAY_TO_STRING(ARRAY_AGG(DISTINCT conversion_name ORDER BY conversion_name), " | ") AS inapp_target_conversions,
            ARRAY_TO_STRING(ARRAY_AGG(DISTINCT conversion_source ORDER BY conversion_source), " | ") AS inapp_conversion_sources,
            COUNT(DISTINCT conversion_id) AS n_of_inapp_target_conversions
        FROM RawConversionIds
        WHERE conversion_type != "DOWNLOAD"
        GROUP BY 1
    )
    SELECT
        campaign_id,
        ANY_VALUE(campaign_sub_type) AS campaign_sub_type,
        ANY_VALUE(app_id) AS app_id,
        ANY_VALUE(app_store) AS app_store,
        ANY_VALUE(bidding_strategy) AS bidding_strategy,
        ANY_VALUE(start_date) AS start_date,
        ANY_VALUE(install_conversion_id) AS install_converion_id,
        ANY_VALUE(inapp_conversion_ids) AS inapp_conversion_ids,
        COALESCE(ANY_VALUE(inapp_target_conversions), ANY_VALUE(install_target_conversions)) AS target_conversions,
        COALESCE(ANY_VALUE(inapp_conversion_sources), ANY_VALUE(install_conversion_sources)) AS conversion_sources,
        COALESCE(ANY_VALUE(n_of_inapp_target_conversions), ANY_VALUE(n_of_install_target_conversions)) AS n_of_target_conversions
    FROM RawConversionIds
    LEFT JOIN InstallConversionId USING(campaign_id)
    LEFT JOIN InappConversionIds USING(campaign_id)
    GROUP BY 1
);

-- Campaign level geo and language targeting
CREATE OR REPLACE VIEW `{bq_dataset}.GeoLanguageView` AS (
    SELECT
        COALESCE(CampaignGeoTarget.campaign_id, CampaignLanguages.campaign_id) AS campaign_id,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT country_code ORDER BY country_code), " | ") AS geos,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT language ORDER BY language), " | ") AS languages,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT geo_target ORDER BY geo_target), " | ") AS geo_ids
    FROM {bq_dataset}.campaign_geo_targets AS CampaignGeoTarget
    LEFT JOIN {bq_dataset}.geo_target_constant AS GeoTargetConstant
        ON CampaignGeoTarget.geo_target = CAST(GeoTargetConstant.constant_id AS STRING)
    FULL JOIN {bq_dataset}.campaign_languages AS CampaignLanguages
        ON CampaignGeoTarget.campaign_id = CampaignLanguages.campaign_id
    GROUP BY 1
);


-- Conversion Lag adjustment placeholder data
-- TODO: Once conversion lag adjustment algorithm is ready switch to it.
CREATE OR REPLACE VIEW `{bq_dataset}.ConversionLagAdjustments` AS (
    SELECT
        DATE_SUB(CURRENT_DATE(), INTERVAL lag_day DAY) AS adjustment_date,
        network,
        conversion_id,
        lag_adjustment
    FROM {bq_dataset}.conversion_lag_adjustments
);

CREATE OR REPLACE VIEW `{bq_dataset}.AssetCohorts` AS (
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
    FROM `{bq_dataset}.conversion_lags_*`
    WHERE
        day_of_interaction IS NOT NULL
        AND lag <= 90
    GROUP BY 1, 2, 3, 4, 5
);
