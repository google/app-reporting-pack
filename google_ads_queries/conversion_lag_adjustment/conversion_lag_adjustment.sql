SELECT
	campaign.id AS campaign_id,
	segments.ad_network_type AS network,
	segments.conversion_action AS conversion_id,
	segments.conversion_action_name AS conversion_name,
	segments.conversion_lag_bucket AS conversion_lag_bucket,
	metrics.all_conversions
FROM campaign
WHERE segments.date >= "{start_date}"
AND segments.date <= "{end_date}"
