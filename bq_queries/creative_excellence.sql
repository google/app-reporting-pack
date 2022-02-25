-- Get number of elements in first non-emtpy array.
CREATE TEMP FUNCTION GetNumberOfElements(first_element STRING, second_element STRING, third_element STRING)
RETURNS INT64
AS (
    ARRAY_LENGTH(SPLIT(
        IFNULL(
            IFNULL(first_element, second_element),
            third_element), "|")
        ) - 1
    );

-- Table for creative_excellence for on campaign_id level. Some information on
-- ad_group level is available. Contains aggregated performance data
-- (cost, installs, inapps) for the last 7 days.
CREATE OR REPLACE TABLE {bq_project}.{bq_dataset}.creative_excellence_F
AS (
WITH
    CostDynamicsTable AS (
        SELECT
            ad_group_id,
            ROUND(SUM(cost / 1e5), 2) AS cost_last_7_days
        FROM {bq_project}.{bq_dataset}.ad_group_performance
        WHERE PARSE_DATE("%Y-%m-%d", date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
        GROUP BY 1
    ),
    ConversionSplitTable AS (
        SELECT
            campaign_id,
            ad_group_id,
            SUM(IF(conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
            SUM(IF(conversion_category != "DOWNLOAD", conversions, 0)) AS inapps
        FROM {bq_project}.{bq_dataset}.ad_group_conversion_split
        WHERE PARSE_DATE("%Y-%m-%d", date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
        GROUP BY 1, 2
    )
SELECT
    M.account_id,
    M.account_name,
    M.currency,
    M.campaign_id,
    M.campaign_name,
    M.campaign_status,
    ACS.campaign_sub_type,
    ACS.app_id,
    ACS.app_store,
    ACS.bidding_strategy,
    ARRAY_TO_STRING(ACS.target_conversions, " | ")  AS target_conversions,
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    ARRAY_LENGTH(ACS.target_conversions) AS n_of_target_conversions,
    ROUND(B.budget_amount / 1e5, 2) AS budget_amount,
    ROUND(B.target_cpa / 1e5, 2) AS target_cpa,
    ROUND(B.target_roas / 1e5, 2) AS target_roas,
    CASE
        WHEN ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST" AND B.budget_amount/B.target_cpa >= 50 THEN "OK"
        WHEN ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST" AND B.budget_amount/B.target_cpa < 50 THEN "50x needed"
        WHEN ACS.bidding_strategy = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST" AND B.budget_amount/B.target_cpa >= 10 THEN "OK"
        WHEN ACS.bidding_strategy = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST" AND B.budget_amount/B.target_cpa < 10 THEN "10x needed"
        ELSE "Not Applicable"
        END AS enough_budget,
    GetNumberOfElements(install_videos, engagement_videos, pre_registration_videos) AS n_videos,
    GetNumberOfElements(install_images, engagement_images, pre_registration_images) AS n_images,
    GetNumberOfElements(install_headlines, engagement_headlines, pre_registration_headlines) AS n_headlines,
    GetNumberOfElements(install_descriptions, engagement_descriptions, pre_registration_descriptions) AS n_descriptions,
    ARRAY_LENGTH(SPLIT(install_media_bundles, "|")) - 1 AS n_html5,
    S.ad_strength AS ad_strength,
    IFNULL(C.cost_last_7_days, 0) AS cost_last_7_days,
    IFNULL(
        IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST", Conv.installs, Conv.inapps),
        0) AS conversions_last_7_days,
    CASE
        WHEN ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST" AND SUM(Conv.installs) OVER (PARTITION BY Conv.campaign_id) > 10
            THEN TRUE
        WHEN ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST" AND SUM(Conv.inapps) OVER (PARTITION BY Conv.campaign_id) > 10
            THEN TRUE
        ELSE FALSE
        END AS enough_conversions,
    0 AS average_budget_7_days,
    0 AS average_bid_7_days,
    0 AS dramatic_bid_changes,
    0 AS dramatic_budget_changes
FROM {bq_project}.{bq_dataset}.account_campaign_ad_group_mapping AS M
LEFT JOIN `{bq_project}.{bq_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN {bq_project}.{bq_dataset}.bid_budget AS B
    ON M.campaign_id = B.campaign_id
LEFT JOIN {bq_project}.{bq_dataset}.asset_structure AS S
  ON M.ad_group_id = S.ad_group_id
LEFT JOIN CostDynamicsTable AS C
  ON M.ad_group_id = C.ad_group_id
LEFT JOIN ConversionSplitTable AS Conv
  ON M.campaign_id = Conv.campaign_id
  AND M.ad_group_id = Conv.ad_group_id
);
