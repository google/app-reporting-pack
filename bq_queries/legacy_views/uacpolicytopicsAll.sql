CREATE OR REPLACE VIEW {legacy_dataset}.uacpolicytopicsAll AS
SELECT
    Day,
    campaign_id AS CampaignID,
    campaign_sub_type AS CampaignSubType,
    "" AS CID,
    account_name AS AccountName,
    account_id,
    campaign_name AS CampaignName,
    CASE campaign_status
        WHEN "ENABLED" THEN "Active"
        WHEN "PAUSED" THEN "Paused"
        WHEN "REMOVED" THEN "Deleted"
    END AS CampaignStatus,
    ad_group_name AS AdGroup,
    ad_group_id AS AdGroupId,
    CASE ad_group_status
        WHEN "ENABLED" THEN "Active"
        WHEN "PAUSED" THEN "Paused"
        WHEN "REMOVED" THEN "Deleted"
    END AS AdGroupStatus,
    app_store AS Store,
    app_id AS AppId,
    bidding_strategy AS UACType,
    geos AS country_code,
    languages AS Language,
    target_conversions AS TargetConversions,
    disapproval_level AS AffectedCreative,
    "" AS LinkToCampaign,
    policy_summary AS evidence_list,
    asset AS AdAsset,
    asset_id,
    asset_link AS Link,
    approval_status AS PolicyTopicType,
    "" AS PolicyURL,
    policy_topics AS PolicyTopic
FROM {target_dataset}.approval_statuses;
