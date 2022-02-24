CREATE OR REPLACE TABLE {bq_project}.{bq_dataset}.network_split_F
AS (
WITH GeoLanguageTable AS (
    SELECT
        campaign_id,
        ARRAY_AGG(DISTINCT geo_target ORDER BY geo_target) AS geos,
        ARRAY_AGG(DISTINCT language ORDER BY language) AS languages
    FROM {bq_project}.{bq_dataset}.campaign_geo_language_target
    GROUP BY 1
),
    AppCampaignSettingsTable AS (
        SELECT
            campaign_id,
            campaign_sub_type,
            app_id,
            app_store,
            bidding_strategy,
            ARRAY_AGG(conversion_source ORDER BY conversion_source) AS conversion_sources,
            ARRAY_AGG(conversion_name ORDER BY conversion_name) AS target_conversions
        FROM {bq_project}.{bq_dataset}.app_campaign_settings
        GROUP BY 1, 2, 3, 4, 5
    )
SELECT
    PARSE_DATE("%Y-%m-%d", AP.date) AS day,
    M.account_id,
    M.account_name,
    M.currency,
    M.campaign_id,
    M.campaign_name,
    M.campaign_status,
    ACS.campaign_sub_type,
    ARRAY_TO_STRING(G.geos, " | ") AS geos,
    ARRAY_TO_STRING(G.languages, " | ") AS languages,
    ACS.app_id,
    ACS.app_store,
    ACS.bidding_strategy,
    ARRAY_TO_STRING(ACS.target_conversions, " | ")  AS target_conversions,
    "" AS firebase_bidding_status,
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    AP.network AS network,
    SUM(AP.clicks) AS clicks,
    SUM(AP.impressions) AS impressions,
    ROUND(SUM(AP.cost) / 1e5, 2) AS cost,
    SUM(AP.view_through_conversions) AS view_through_conversions
FROM {bq_project}.{bq_dataset}.ad_group_performance AS AP
LEFT JOIN {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping AS M
  ON AP.ad_group_id = M.ad_group_id
LEFT JOIN AppCampaignSettingsTable AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN GeoLanguageTable AS G
  ON M.campaign_id =  G.campaign_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
