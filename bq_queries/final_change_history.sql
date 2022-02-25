-- Contains daily changes (bids, budgets, assets, etc) on campaign level.
CREATE OR REPLACE TABLE {bq_project}.{bq_dataset}.final_change_history_F
AS (
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
    ARRAY_LENGTH(ACS.target_conversions) AS n_of_target_conversions,
    ROUND(B.budget_amount / 1e5, 2) AS current_budget_amount,
    ROUND(B.target_cpa / 1e5, 2) AS current_target_cpa,
    ROUND(B.target_roas / 1e5, 2) AS current_target_roas,
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
LEFT JOIN {bq_project}.{bq_dataset}.bid_budget AS B
    ON M.campaign_id = B.campaign_id
LEFT JOIN `{bq_project}.{bq_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_project}.{bq_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32);
