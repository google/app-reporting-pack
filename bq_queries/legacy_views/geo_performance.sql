CREATE OR REPLACE VIEW {legacy_dataset}.geo_performance AS
SELECT
    Day,
    campaign_id AS CampaignID,
    account_name AS AccountName,
    "" AS CID,
    app_id AS AppId,
    account_id,
    campaign_sub_type AS CampaignSubType,
    campaign_name AS CampaignName,
    campaign_status AS CampaignStatus,
    ad_group_name AS AdGroupName,
    ad_group_id AS AdGroupId,
    country_code AS Country,
    target_conversions AS TargetConversions,
    Network,
    clicks,
    impressions,
    cost,
    installs,
    inapps,
    conversions_value AS conversion_value
FROM {target_dataset}.geo_performance;
