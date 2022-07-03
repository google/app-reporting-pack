-- Contains ad group level performance segmented by network (Search, Display, YouTube).
CREATE OR REPLACE TABLE {bq_project}.{target_dataset}.network_split
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
    IFNULL(G.geos, "All") AS geos,
    IFNULL(G.languages, "All") AS languages,
    ACS.app_id,
    ACS.app_store,
    ACS.bidding_strategy,
    ACS.target_conversions,
    "" AS firebase_bidding_status,
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    AP.network AS network,
    SUM(AP.clicks) AS clicks,
    SUM(AP.impressions) AS impressions,
    `{bq_project}.{target_dataset}.NormalizeMillis`(SUM(AP.cost)) AS cost,
    SUM(IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
        IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0),
        IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0))) AS conversions,
    SUM(IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
    SUM(
        IF(LagAdjustmentsInstalls.lag_adjustment IS NULL,
            IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0),
            ROUND(IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0) / LagAdjustmentsInstalls.lag_adjustment))
    ) AS installs_adjusted,
    SUM(IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0)) AS inapps,
    SUM(
        IF(LagAdjustmentsInapps.lag_adjustment IS NULL,
            IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0),
            ROUND(IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0) / LagAdjustmentsInapps.lag_adjustment))
    ) AS inapps_adjusted,
    SUM(AP.view_through_conversions) AS view_through_conversions,
    SUM(AP.video_views) AS video_views,
    SUM(AP.conversions_value) AS conversions_value
FROM {bq_project}.{bq_dataset}.ad_group_performance AS AP
LEFT JOIN {bq_project}.{bq_dataset}.ad_group_conversion_split AS ConvSplit
    USING(date, ad_group_id, network)
LEFT JOIN {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping AS M
  ON AP.ad_group_id = M.ad_group_id
LEFT JOIN `{bq_project}.{target_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_project}.{target_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
LEFT JOIN `{bq_project}.{target_dataset}.ConversionLagAdjustments` AS LagAdjustmentsInstalls
    ON PARSE_DATE("%Y-%m-%d", AP.date) = LagAdjustmentsInstalls.adjustment_date
        AND AP.network = LagAdjustmentsInstalls.network
        AND ACS.install_conversion_id = LagAdjustmentsInstalls.conversion_id
LEFT JOIN `{bq_project}.{target_dataset}.ConversionLagAdjustments` AS LagAdjustmentsInapps
    ON PARSE_DATE("%Y-%m-%d", AP.date) = LagAdjustmentsInapps.adjustment_date
        AND AP.network = LagAdjustmentsInapps.network
        AND ACS.inapp_conversion_id = LagAdjustmentsInapps.conversion_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
