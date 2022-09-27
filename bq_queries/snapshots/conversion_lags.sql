-- Contains conversions and their values on a given day with automatically calculated lag number
CREATE OR REPLACE TABLE {bq_dataset}.conversion_lags_{date_iso}
AS (
SELECT
    PARSE_DATE("%Y-%m-%d", AP.date) AS day_of_interaction,
    DATE_DIFF(
        CURRENT_DATE(),
        PARSE_DATE("%Y-%m-%d", AP.date),
        DAY) AS lag,
    M.ad_group_id,
    AP.asset_id,
    AP.network AS network,
    SUM(AP.installs) AS installs,
    SUM(AP.inapps) AS inapps,
    SUM(AP.view_through_conversions) AS view_through_conversions,
    SUM(AP.conversions_value) AS conversions_value
FROM {bq_dataset}.asset_performance AS AP
LEFT JOIN {bq_dataset}.account_campaign_ad_group_mapping AS M
  ON AP.ad_group_id = M.ad_group_id
GROUP BY 1, 2, 3, 4, 5);
