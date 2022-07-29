-- Contains daily changes (bids, budgets, assets, etc) on campaign level.
CREATE OR REPLACE TABLE {bq_project}.{target_dataset}.final_change_history
AS (
    WITH CampaignPerformance AS (
        SELECT
            PARSE_DATE("%Y-%m-%d", date) AS day,
            M.campaign_id,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            SUM(cost) AS cost,
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
            SUM(view_through_conversions) AS view_through_conversions,
            SUM(AP.conversions_value) AS conversions_value
        FROM {bq_project}.{bq_dataset}.ad_group_performance AS AP
        LEFT JOIN {bq_project}.{bq_dataset}.ad_group_conversion_split AS ConvSplit
            USING(date, ad_group_id, network)
        LEFT JOIN {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping AS M
            USING(ad_group_id)
        LEFT JOIN `{bq_project}.{target_dataset}.AppCampaignSettingsView` AS ACS
          ON M.campaign_id = ACS.campaign_id
        LEFT JOIN `{bq_project}.{target_dataset}.ConversionLagAdjustments` AS LagAdjustmentsInstalls
            ON PARSE_DATE("%Y-%m-%d", AP.date) = LagAdjustmentsInstalls.adjustment_date
                AND AP.network = LagAdjustmentsInstalls.network
                AND ACS.install_conversion_id = LagAdjustmentsInstalls.conversion_id
        LEFT JOIN `{bq_project}.{target_dataset}.ConversionLagAdjustments` AS LagAdjustmentsInapps
            ON PARSE_DATE("%Y-%m-%d", AP.date) = LagAdjustmentsInapps.adjustment_date
                AND AP.network = LagAdjustmentsInapps.network
                AND ACS.inapp_conversion_id = LagAdjustmentsInapps.conversion_id
        GROUP BY 1, 2
    ),
    CampaignMapping AS (
        SELECT DISTINCT
            account_id,
            account_name,
            currency,
            campaign_id,
            campaign_name,
            campaign_status
        FROM {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping
    )
SELECT
    CP.day,
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
    ACS.start_date AS start_date,
    DATE_DIFF(
        CURRENT_DATE(),
        PARSE_DATE("%Y-%m-%d", ACS.start_date),
        DAY) AS days_since_start_date,
    ACS.n_of_target_conversions,
    `{bq_project}.{bq_dataset}.NormalizeMillis`(B.budget_amount) AS current_budget_amount,
    `{bq_project}.{bq_dataset}.NormalizeMillis`(B.target_cpa) AS current_target_cpa,
    B.target_roas AS current_target_roas,
    `{bq_project}.{bq_dataset}.NormalizeMillis`(BidBudgetHistory.budget_amount) AS budget,
    `{bq_project}.{bq_dataset}.NormalizeMillis`(BidBudgetHistory.target_cpa) AS target_cpa,
    BidBudgetHistory.target_roas AS target_roas,
    -- If cost for a given day is higher than allocated budget than is_budget_limited = 1
    IF(CP.cost > BidBudgetHistory.budget_amount, 1, 0) AS is_budget_limited,
    -- If cost for a given day is 20% higher than allocated budget than is_budget_overshooting = 1
    IF(CP.cost > BidBudgetHistory.budget_amount * 1.2, 1, 0) AS is_budget_overshooting,
    -- If cost for a given day is 50% lower than allocated budget than is_budget_underspend = 1
    IF(CP.cost < BidBudgetHistory.budget_amount * 0.5, 1, 0) AS is_budget_underspend,
    -- If CPA for a given day is 10% higher than target_cpa than is_cpa_overshooting = 1
    IF(SAFE_DIVIDE(CP.cost, CP.conversions) > BidBudgetHistory.target_cpa * 1.1, 1, 0) AS is_cpa_overshooting,
    0 AS text_changes,
    0 AS image_changes,
    0 AS html5_changes,
    0 AS video_changes,
    0 AS geo_changes,
    0 AS ad_group_added,
    0 AS ad_group_resumed,
    0 AS ad_group_paused,
    0 AS ad_group_deleted,
    CASE
        WHEN BidBudgetHistory.target_cpa = LAG(BidBudgetHistory.target_cpa)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC)
        OR LAG(BidBudgetHistory.target_cpa)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC) IS NULL
        THEN 0
    ELSE 1
    END AS bid_changes,
    CASE
        WHEN BidBudgetHistory.budget_amount = LAG(BidBudgetHistory.budget_amount)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC)
        OR LAG(BidBudgetHistory.budget_amount)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC) IS NULL
        THEN 0
    ELSE 1
    END AS budget_changes,
    CP.clicks,
    CP.impressions,
    `{bq_project}.{bq_dataset}.NormalizeMillis`(CP.cost) AS cost,
    IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
        CP.installs, CP.inapps) AS conversions,
    IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
        CP.installs_adjusted, CP.inapps_adjusted) AS conversions_adjusted,
    CP.installs,
    CP.installs_adjusted,
    CP.inapps,
    CP.inapps_adjusted,
    CP.view_through_conversions,
    CP.conversions_value
FROM CampaignPerformance AS CP
LEFT JOIN CampaignMapping AS M
    ON CP.campaign_id = M.campaign_id
LEFT JOIN {bq_project}.{bq_dataset}.bid_budget AS B
    ON CP.campaign_id = B.campaign_id
LEFT JOIN `{bq_project}.{target_dataset}.bid_budgets_*` AS BidBudgetHistory
    ON M.campaign_id = BidBudgetHistory.campaign_id
        AND CP.day = BidBudgetHistory.day
LEFT JOIN `{bq_project}.{target_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_project}.{target_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id);
