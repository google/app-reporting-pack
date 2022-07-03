CREATE OR REPLACE VIEW {bq_project}.{legacy_dataset}.uac_perf_grouping_history AS
SELECT
    Day,
    account_name AS AccountName,
    account_id,
    Currency,
    campaign_sub_type AS CampaignSubType,
    campaign_name AS CampaignName,
    campaign_id AS CampaignID,
    ad_group_id AS AdGroupId,
    ad_group_name AS AdGroupName,
    performance_label AS PerformanceGrouping,
    N,
    app_store AS Store,
    app_id AS AppId,
    bidding_strategy AS UACType,
    geos AS country_code,
    languages AS Language,
    target_conversions AS TargetConversions
FROM {bq_project}.{target_dataset}.performance_grouping;

