# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

-- Final Table for Age Group Performance
CREATE OR REPLACE TABLE {target_dataset}.age_group_performance
AS (
    WITH
    	 ConversionsTable AS 
    	 (
	        SELECT
	            date,
	            network,
	            campaign_id,
	            age_range,
	            SUM(conversions) AS conversions,
	            SUM(IF(conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
	            SUM(IF(conversion_category != "DOWNLOAD", conversions, 0)) AS inapps
	        FROM 
	        	`{bq_dataset}.age_range_conversion_split`
	        GROUP BY 
	        	1, 2, 3, 4
    	)
    
    SELECT
        PARSE_DATE("%Y-%m-%d", ARP.date) AS day,
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
        ACS.target_conversions,
        ACS.start_date AS start_date,
        ARP.network,
        age_range,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        `{bq_dataset}.NormalizeMillis`(SUM(ARP.cost)) AS cost,
        SUM(ConvSplit.installs) AS installs,
        SUM(ConvSplit.inapps) AS inapps,
        SUM(video_views) AS video_views,
        SUM(interactions) AS interactions,
        SUM(engagements) AS engagements,
        SUM(conversions_value) AS conversions_value,
        SUM(view_through_conversions) AS view_through_conversions,
        SUM(video_quartile_p25_rate) AS video_quartile_p25_rate,
        SUM(video_quartile_p50_rate) AS video_quartile_p50_rate,
        SUM(video_quartile_p75_rate) AS video_quartile_p75_rate,
        SUM(video_quartile_p100_rate) AS video_quartile_p100_rate
    FROM 
    	`{bq_dataset}.age_range_performance` AS ARP
    LEFT JOIN 
    	ConversionsTable AS ConvSplit
        USING (campaign_id, network, date, age_range)
    LEFT JOIN 
    	`{bq_dataset}.account_campaign_ad_group_mapping` AS M
    	USING (campaign_id)
    LEFT JOIN 
    	`{bq_dataset}.AppCampaignSettingsView` AS ACS
		USING (campaign_id)
    GROUP BY 
    	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
);

--Final Table for Gender Group Performance
CREATE OR REPLACE TABLE {target_dataset}.gender_group_performance
AS (
    WITH
    	 ConversionsTable AS 
    	 (
	        SELECT
	            date,
	            network,
	            campaign_id,
	            gender_group,
	            SUM(conversions) AS conversions,
	            SUM(IF(conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
	            SUM(IF(conversion_category != "DOWNLOAD", conversions, 0)) AS inapps
	        FROM 
	        	`{bq_dataset}.gender_group_conversion_split`
	        GROUP BY 
	        	1, 2, 3, 4
    	)
    
    SELECT
        PARSE_DATE("%Y-%m-%d", GGP.date) AS day,
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
        ACS.target_conversions,
        ACS.start_date AS start_date,
        GGP.network,
        gender_group,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        `{bq_dataset}.NormalizeMillis`(SUM(GGP.cost)) AS cost,
        SUM(ConvSplit.installs) AS installs,
        SUM(ConvSplit.inapps) AS inapps,
        SUM(video_views) AS video_views,
        SUM(interactions) AS interactions,
        SUM(engagements) AS engagements,
        SUM(conversions_value) AS conversions_value,
        SUM(view_through_conversions) AS view_through_conversions,
        SUM(video_quartile_p25_rate) AS video_quartile_p25_rate,
        SUM(video_quartile_p50_rate) AS video_quartile_p50_rate,
        SUM(video_quartile_p75_rate) AS video_quartile_p75_rate,
        SUM(video_quartile_p100_rate) AS video_quartile_p100_rate
    FROM 
    	`{bq_dataset}.gender_group_performance` AS GGP
    LEFT JOIN 
    	ConversionsTable AS ConvSplit
        USING (campaign_id, network, date, gender_group)
    LEFT JOIN 
    	`{bq_dataset}.account_campaign_ad_group_mapping` AS M
    	USING (campaign_id)
    LEFT JOIN 
    	`{bq_dataset}.AppCampaignSettingsView` AS ACS
		USING (campaign_id)
    GROUP BY 
    	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
);

--Final Table for Income Group Performance
CREATE OR REPLACE TABLE {target_dataset}.income_group_performance
AS (
    WITH
    	 ConversionsTable AS 
    	 (
	        SELECT
	            date,
	            network,
	            campaign_id,
	            income_range,
	            SUM(conversions) AS conversions,
	            SUM(IF(conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
	            SUM(IF(conversion_category != "DOWNLOAD", conversions, 0)) AS inapps
	        FROM 
	        	`{bq_dataset}.income_range_conversion_split`
	        GROUP BY 
	        	1, 2, 3, 4
    	)
    
    SELECT
        PARSE_DATE("%Y-%m-%d", IRP.date) AS day,
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
        ACS.target_conversions,
        ACS.start_date AS start_date,
        IRP.network,
        income_range,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        `{bq_dataset}.NormalizeMillis`(SUM(IRP.cost)) AS cost,
        SUM(ConvSplit.installs) AS installs,
        SUM(ConvSplit.inapps) AS inapps,
        SUM(video_views) AS video_views,
        SUM(interactions) AS interactions,
        SUM(engagements) AS engagements,
        SUM(conversions_value) AS conversions_value,
        SUM(view_through_conversions) AS view_through_conversions,
        SUM(video_quartile_p25_rate) AS video_quartile_p25_rate,
        SUM(video_quartile_p50_rate) AS video_quartile_p50_rate,
        SUM(video_quartile_p75_rate) AS video_quartile_p75_rate,
        SUM(video_quartile_p100_rate) AS video_quartile_p100_rate
    FROM 
    	`{bq_dataset}.income_range_performance` AS IRP
    LEFT JOIN 
    	ConversionsTable AS ConvSplit
        USING (campaign_id, network, date, income_range)
    LEFT JOIN 
    	`{bq_dataset}.account_campaign_ad_group_mapping` AS M
    	USING (campaign_id)
    LEFT JOIN 
    	`{bq_dataset}.AppCampaignSettingsView` AS ACS
		USING (campaign_id)
    GROUP BY 
    	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
);

--Final Table for Income Group Performance
CREATE OR REPLACE TABLE {target_dataset}.parent_group_performance
AS (
    WITH
    	 ConversionsTable AS 
    	 (
	        SELECT
	            date,
	            network,
	            campaign_id,
	            parent_group,
	            SUM(conversions) AS conversions,
	            SUM(IF(conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
	            SUM(IF(conversion_category != "DOWNLOAD", conversions, 0)) AS inapps
	        FROM 
	        	`{bq_dataset}.parent_group_conversion_split`
	        GROUP BY 
	        	1, 2, 3, 4
    	)
    
    SELECT
        PARSE_DATE("%Y-%m-%d", PGP.date) AS day,
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
        ACS.target_conversions,
        ACS.start_date AS start_date,
        PGP.network,
        parent_group,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        `{bq_dataset}.NormalizeMillis`(SUM(PGP.cost)) AS cost,
        SUM(ConvSplit.installs) AS installs,
        SUM(ConvSplit.inapps) AS inapps,
        SUM(video_views) AS video_views,
        SUM(interactions) AS interactions,
        SUM(engagements) AS engagements,
        SUM(conversions_value) AS conversions_value,
        SUM(view_through_conversions) AS view_through_conversions,
        SUM(video_quartile_p25_rate) AS video_quartile_p25_rate,
        SUM(video_quartile_p50_rate) AS video_quartile_p50_rate,
        SUM(video_quartile_p75_rate) AS video_quartile_p75_rate,
        SUM(video_quartile_p100_rate) AS video_quartile_p100_rate
    FROM 
    	`{bq_dataset}.parent_group_performance` AS PGP
    LEFT JOIN 
    	ConversionsTable AS ConvSplit
        USING (campaign_id, network, date, parent_group)
    LEFT JOIN 
    	`{bq_dataset}.account_campaign_ad_group_mapping` AS M
    	USING (campaign_id)
    LEFT JOIN 
    	`{bq_dataset}.AppCampaignSettingsView` AS ACS
		USING (campaign_id)
    GROUP BY 
    	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
);
