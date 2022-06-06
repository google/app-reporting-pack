-- Contains ad group level performance segmented by network (Search, Display, YouTube).
CREATE OR REPLACE TABLE {bq_project}.{bq_dataset}.network_split_F
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
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    AP.network AS network,
    SUM(AP.clicks) AS clicks,
    SUM(AP.impressions) AS impressions,
    `{bq_project}.{bq_dataset}.NormalizeMillis`(SUM(AP.cost)) AS cost,
    SUM(AP.view_through_conversions) AS view_through_conversions
FROM {bq_project}.{bq_dataset}.ad_group_performance AS AP
LEFT JOIN {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping AS M
  ON AP.ad_group_id = M.ad_group_id
LEFT JOIN `{bq_project}.{bq_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_project}.{bq_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
