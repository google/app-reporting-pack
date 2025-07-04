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

-- Contains ad group level performance segmented by network (Search, Display, YouTube).
-- Added Cohort Performance view on the Campaign and AgGroup Level
CREATE TEMP FUNCTION GetCohort(arr ARRAY<FLOAT64>, day INT64)
RETURNS FLOAT64
AS (
    arr[SAFE_OFFSET(day)]
);
CREATE OR REPLACE TABLE {target_dataset}.network_split
AS (
    WITH
    	ConversionsTable AS 
    	(
        SELECT
            date,
            network,
            ad_group_id,
            SUM(conversions) AS conversions,
            SUM(conversions_value) AS conversions_value,
            SUM(IF(conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
            SUM(IF(conversion_category != "DOWNLOAD", conversions, 0)) AS inapps  
        FROM 
        `{bq_dataset}.ad_group_conversion_split`
        GROUP BY 
        	1, 2, 3
    	),
    MappingTable AS 
    (
     SELECT
         ad_group_id,
         ANY_VALUE(ad_group_name) AS ad_group_name,
         ANY_VALUE(ad_group_status) AS ad_group_status,
         ANY_VALUE(campaign_id) AS campaign_id,
         ANY_VALUE(campaign_name) AS campaign_name,
         ANY_VALUE(campaign_status) AS campaign_status,
         ANY_VALUE(account_id) AS account_id,
         ANY_VALUE(account_name) AS account_name,
         ANY_VALUE(currency) AS currency
     FROM 
     	`{bq_dataset}.account_campaign_ad_group_mapping`
     GROUP BY 
     	1
     ),
    FinalTablePrep AS
    (
    	SELECT
   		    PARSE_DATE("%Y-%m-%d", ICP.date) AS day,
   		    M.account_id,
   		    M.account_name,
   		    M.currency,
   		    M.campaign_id,
   		    M.campaign_name,
   		    M.campaign_status,
   		    ACS.campaign_sub_type,
   		    IFNULL(G.geos, "All") AS geos,
   		    IFNULL(G.languages, "All") AS languages,
   		    ACS.app_id,
   		    ACS.app_store,
   		    ACS.bidding_strategy,
   		    ACS.target_conversions,
   		    "" AS firebase_bidding_status,
   		    M.ad_group_id,
   		    M.ad_group_name,
   		    M.ad_group_status,
   		    ICP.network,
   		    SUM(ICP.clicks) AS clicks,
   		    SUM(ICP.impressions) AS impressions,
   		    `{bq_dataset}.NormalizeMillis`(SUM(ICP.cost)) AS cost,
   		    SUM(ICP.video_views) AS video_views,
   	        SUM(ICP.interactions) AS interactions,
   	        SUM(ICP.engagements) AS engagements,
   		    SUM(ICP.view_through_conversions) AS view_through_conversions,
   	        SUM(ICP.video_quartile_p25_rate) AS video_quartile_p25_rate,
   	        SUM(ICP.video_quartile_p50_rate) AS video_quartile_p50_rate,
   	        SUM(ICP.video_quartile_p75_rate) AS video_quartile_p75_rate,
   	        SUM(ICP.video_quartile_p100_rate) AS video_quartile_p100_rate,
   		    SUM(CS.conversions) AS conversions,
   		    SUM(CS.installs) AS installs,
   		    SUM(CS.inapps) AS inapps,
   		    SUM(ICP.conversions_value) AS conversions_value
   		FROM 
   			`{bq_dataset}.ad_group_performance_extended` AS ICP
   		LEFT JOIN 
   			ConversionsTable AS CS
   		    USING(date, ad_group_id, network)
   		LEFT JOIN 
   			MappingTable AS M
   			ON ICP.ad_group_id = M.ad_group_id
   		LEFT JOIN 
   			`{bq_dataset}.AppCampaignSettingsView` AS ACS
   		 	ON M.campaign_id = ACS.campaign_id
   		LEFT JOIN 
   			`{bq_dataset}.GeoLanguageView` AS G
   		  	ON M.campaign_id =  G.campaign_id
   		GROUP BY 
   			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    )
    
	SELECT
	    day,
	    account_id,
	    account_name,
	    currency,
	    campaign_id,
	    campaign_name,
	    campaign_status,
	    campaign_sub_type,
	    geos,
	    IFNULL(languages, "All") AS languages,
	    app_id,
	    app_store,
	    bidding_strategy,
	    target_conversions,
	    firebase_bidding_status,
	    ad_group_id,
	    ad_group_name,
	    ad_group_status,
	    `{bq_dataset}.ConvertAdNetwork`(network) AS network,
	    SUM(clicks) AS clicks,
	    SUM(impressions) AS impressions,
	    SUM(cost) AS cost,
	    SUM(video_views) AS video_views,
        SUM(interactions) AS interactions,
        SUM(engagements) AS engagements,
	    SUM(view_through_conversions) AS view_through_conversions,
        SUM(video_quartile_p25_rate) AS video_quartile_p25_rate,
        SUM(video_quartile_p50_rate) AS video_quartile_p50_rate,
        SUM(video_quartile_p75_rate) AS video_quartile_p75_rate,
        SUM(video_quartile_p100_rate) AS video_quartile_p100_rate,
	    SUM(conversions) AS conversions,
	    SUM(installs) AS installs,
	    SUM(inapps) AS inapps,
	    SUM(conversions_value) AS conversions_value,
	    {% for day in cohort_days %}
	            SUM(GetCohort(CC.lag_data.installs, {{day}})) AS installs_{{day}}_day,
	            SUM(GetCohort(CC.lag_data.inapps, {{day}})) AS inapps_{{day}}_day,
	            SUM(GetCohort(CC.lag_data.conversions_value, {{day}})) AS conversions_value_{{day}}_day,
	    {% endfor %}
	FROM 
		FinalTablePrep
	LEFT JOIN 
	(
		SELECT
			day_of_interaction AS day,
			ad_group_id,
			network,
			lag_data
		FROM
			`{bq_dataset}.CampaignAdGroupCohorts`	
	) AS CC
	USING (day, ad_group_id, network)
	GROUP BY 
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
	);