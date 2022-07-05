-- Represents basic app campaign settings that are used when creating
-- final tables for dashboards
CREATE OR REPLACE VIEW `{bq_project}.{target_dataset}.AppCampaignSettingsView`
AS (
    SELECT
        campaign_id,
        campaign_sub_type,
        app_id,
        app_store,
        bidding_strategy,
        start_date,
        IF(conversion_type = "DOWNLOAD", conversion_id, NULL) AS install_conversion_id,
        IF(conversion_type != "DOWNLOAD", conversion_id, NULL) AS inapp_conversion_id,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT conversion_source ORDER BY conversion_source), "|") AS conversion_sources,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT conversion_name ORDER BY conversion_name), " | ") AS target_conversions,
        COUNT(conversion_name) AS n_of_target_conversions
    FROM {bq_project}.{bq_dataset}.app_campaign_settings
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
);

-- Campaign level geo and language targeting
CREATE OR REPLACE VIEW `{bq_project}.{target_dataset}.GeoLanguageView` AS (
    SELECT
        COALESCE(CampaignGeoTarget.campaign_id, CampaignLanguages.campaign_id) AS campaign_id,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT country_code ORDER BY country_code), " | ") AS geos,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT language ORDER BY language), " | ") AS languages
    FROM {bq_project}.{bq_dataset}.campaign_geo_targets AS CampaignGeoTarget
    LEFT JOIN {bq_project}.{bq_dataset}.geo_target_constant AS GeoTargetConstant
        ON CampaignGeoTarget.geo_target = CAST(GeoTargetConstant.constant_id AS STRING)
    FULL JOIN {bq_project}.{bq_dataset}.campaign_languages AS CampaignLanguages
        ON CampaignGeoTarget.campaign_id = CampaignLanguages.campaign_id
    GROUP BY 1
);


-- Conversion Lag adjustment placeholder data
-- TODO: Once conversion lag adjustment algorithm is ready switch to it.
CREATE OR REPLACE VIEW `{bq_project}.{target_dataset}.ConversionLagAdjustments` AS (
    SELECT
        DATE_SUB(CURRENT_DATE(), INTERVAL lag_day DAY) AS adjustment_date,
        network,
        conversion_id,
        lag_adjustment
    FROM {bq_project}.{bq_dataset}.conversion_lag_adjustments
);

CREATE OR REPLACE VIEW `{bq_project}.{target_dataset}.AssetCohorts` AS (
    SELECT
        day_of_interaction,
        ad_group_id,
        asset_id,
        network,
        STRUCT(
            ARRAY_AGG(lag ORDER BY lag) AS lags,
            ARRAY_AGG(installs ORDER BY lag) AS installs,
            ARRAY_AGG(inapps ORDER BY lag) AS inapps,
            ARRAY_AGG(conversions_value ORDER BY lag) AS conversions_value,
            ARRAY_AGG(view_through_conversions ORDER BY lag) AS view_through_conversions
        ) AS lag_data
    FROM `{bq_project}.{bq_dataset}.conversion_lags_*`
    WHERE
        day_of_interaction IS NOT NULL
        AND lag <= 90
    GROUP BY 1, 2, 3, 4
);
