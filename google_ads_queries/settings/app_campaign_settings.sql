SELECT
    campaign.id AS campaign_id,
    campaign.advertising_channel_sub_type AS campaign_sub_type,
    campaign.app_campaign_setting.app_id AS app_id,
    campaign.app_campaign_setting.app_store AS app_store,
    campaign.app_campaign_setting.bidding_strategy_goal_type AS bidding_strategy,
    campaign.start_date AS start_date,
    segments.external_conversion_source AS conversion_source,
    segments.conversion_action_name AS conversion_name,
    segments.conversion_action_category AS conversion_type,
    segments.conversion_action~0 AS conversion_id
FROM
    campaign
WHERE campaign.advertising_channel_type = "MULTI_CHANNEL"
