CREATE OR REPLACE TABLE {bq_project}.{bq_dataset}.final_change_history_F
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
            start_date,
            ARRAY_AGG(conversion_source ORDER BY conversion_source) AS conversion_sources,
            ARRAY_AGG(conversion_name ORDER BY conversion_name) AS target_conversions
        FROM {bq_project}.{bq_dataset}.app_campaign_settings
        GROUP BY 1, 2, 3, 4, 5, 6
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
    ACS.start_date AS start_date,
    0 AS n_target_conversions,
    0 AS current_budget,
    0 AS current_target_cpa,
    0 AS current_target_roas,
    0 AS budget,
    0 AS target_cpa,
    0 AS target_roas,
    0 AS bid_changes,
    0 AS budget_changes,
    0 AS image_changes,
    0 AS text_changes,
    0 AS html5_changes,
    0 AS video_changes,
    0 AS geo_changes,
    0 AS ad_group_added,
    0 AS ad_group_resumed,
    0 AS ad_group_paused,
    0 AS ad_group_deleted,
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
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32);
