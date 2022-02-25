-- Represents basic app campaign settings that are used when creating
-- final tables for dashboards
CREATE OR REPLACE VIEW `{bq_project}.{bq_dataset}.AppCampaignSettingsView`
AS (
    SELECT
        campaign_id,
        campaign_sub_type,
        app_id,
        app_store,
        bidding_strategy,
        start_date,
        ARRAY_AGG(conversion_source ORDER BY conversion_source) AS conversion_sources,
        ARRAY_AGG(conversion_name ORDER BY conversion_name) AS target_conversions
    FROM {bq_project}.{bq_dataset}.app_campaign_settings
    GROUP BY 1, 2, 3, 4, 5, 6
);

-- Campaign level geo and language targeting
CREATE OR REPLACE VIEW `{bq_project}.{bq_dataset}.GeoLanguageView` AS (
    SELECT
        campaign_id,
        ARRAY_AGG(DISTINCT geo_target ORDER BY geo_target) AS geos,
        ARRAY_AGG(DISTINCT language ORDER BY language) AS languages
    FROM {bq_project}.{bq_dataset}.campaign_geo_language_target
    GROUP BY 1
);
