SELECT
    campaign.id AS campaign_id,
    campaign_criterion.location.geo_target_constant AS geo_target,
    campaign_criterion.type AS type,
    language_constant.code AS language
FROM campaign_criterion
WHERE
    campaign.advertising_channel_type = "MULTI_CHANNEL"
    AND campaign_criterion.type IN (
        "LANGUAGE",
        "LOCATION"
    )
    AND campaign_criterion.negative = FALSE
