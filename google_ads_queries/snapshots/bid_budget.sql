SELECT
    campaign.id AS campaign_id,
    campaign_budget.amount_micros AS budget_amount,
    campaign.target_cpa.target_cpa_micros AS target_cpa,
    campaign.target_roas.target_roas AS target_roas
FROM campaign
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND campaign.status = "ENABLED"
