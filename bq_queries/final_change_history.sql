-- Contains daily changes (bids, budgets, assets, etc) on campaign level.
CREATE OR REPLACE TABLE {bq_project}.{target_dataset}.final_change_history
AS (
    WITH CampaignPerformance AS (
        SELECT
            PARSE_DATE("%Y-%m-%d", date) AS day,
            campaign_id,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            `{bq_project}.{target_dataset}.NormalizeMillis`(SUM(cost)) AS cost,
            SUM(view_through_conversions) AS view_through_conversions
        FROM {bq_project}.{bq_dataset}.ad_group_performance
        LEFT JOIN {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping AS M
            USING(ad_group_id)
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
    ACS.n_of_target_conversions,
    `{bq_project}.{target_dataset}.NormalizeMillis`(B.budget_amount) AS current_budget_amount,
    `{bq_project}.{target_dataset}.NormalizeMillis`(B.target_cpa) AS current_target_cpa,
    B.target_roas AS current_target_roas,
    BidBudgetHistory.budget_amount AS budget,
    BidBudgetHistory.target_cpa AS target_cpa,
    BidBudgetHistory.target_roas AS target_roas,
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
