SELECT
    customer.descriptive_name AS account_name,
    customer.currency_code AS currency,
    customer.id AS account_id,
    campaign.id AS campaign_id,
    campaign.name AS campaign_name,
    campaign.status AS campaign_status,
    campaign.start_date AS campaign_start_date,
    ad_group.id AS ad_group_id,
    ad_group.name AS ad_group_name,
    ad_group.status AS ad_group_status
FROM
    ad_group
WHERE campaign.advertising_channel_type = "MULTI_CHANNEL"
