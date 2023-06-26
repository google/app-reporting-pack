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

-- SKAN Only  Geo Performance
CREATE OR REPLACE TABLE {target_dataset}.skan_geo_performance
AS (
    WITH 
          SharePrep AS 
            (
              SELECT
              	campaign_id,
              	country_code,
               	SAFE_DIVIDE(cost,SUM(cost) OVER (campaigns)) AS geo_cost_share,
                SAFE_DIVIDE(impressions,SUM(impressions) OVER (campaigns)) AS geo_impressions_share,
                SAFE_DIVIDE(clicks,SUM(clicks) OVER (campaigns)) AS geo_clicks_share,
                SAFE_DIVIDE(interactions,SUM(interactions) OVER (campaigns)) AS geo_interactions_share,
                SAFE_DIVIDE(conversions,SUM(conversions) OVER (campaigns)) AS geo_conversions_share,
                SAFE_DIVIDE(installs,SUM(installs) OVER (campaigns)) AS geo_installs_share,
                SAFE_DIVIDE(inapps,SUM(inapps) OVER (campaigns)) AS geo_inapps_share,
                SAFE_DIVIDE(conversions_value,SUM(conversions_value) OVER (campaigns)) AS geo_conversion_value_share
        	  FROM
        	 	 `{target_dataset}.geo_performance`
        	  WINDOW campaigns AS (PARTITION BY campaign_id)
        	),
        	
          ShareFinal AS 
           (
           	SELECT
           		campaign_id,
                country_code,
                MIN(geo_cost_share) AS geo_cost_share,
                MIN(geo_impressions_share) AS geo_impressions_share,
                MIN(geo_clicks_share) AS geo_clicks_share,
                MIN(geo_interactions_share) AS geo_interactions_share,
                MIN(geo_conversions_share) AS geo_conversions_share,
                MIN(geo_installs_share) AS geo_installs_share,
                MIN(geo_inapps_share) AS geo_inapps_share,
                MIN(geo_conversion_value_share) AS geo_conversion_value_share,
                AVG(average_geo_share) AS average_geo_share
            FROM
                SharePrep,
                UNNEST ([geo_cost_share,geo_impressions_share, geo_clicks_share, geo_interactions_share, geo_conversions_share, geo_installs_share, geo_inapps_share, geo_conversion_value_share]) AS average_geo_share
            GROUP BY 
            	1,2
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
        app_id,
        bidding_strategy,
        target_conversions,
        start_date,
        country_code,
        SUM(skan_installs) AS skan_installs,
        MIN(geo_cost_share) AS geo_cost_share,
        MIN(geo_impressions_share) AS geo_impressions_share,
        MIN(geo_clicks_share) AS geo_clicks_share,
        MIN(geo_interactions_share) AS geo_interactions_share,
        MIN(geo_conversions_share) AS geo_conversions_share,
        MIN(geo_installs_share) AS geo_installs_share,
        MIN(geo_inapps_share) AS geo_inapps_share,
        MIN(geo_conversion_value_share) AS geo_conversion_value_share,
        AVG(average_geo_share) AS average_geo_share
    FROM
    	`{target_dataset}.geo_performance`
    LEFT JOIN
    	(
    		SELECT
    			PARSE_DATE("%Y-%m-%d", date) AS day,
    			campaign_id,
    			SUM(skan_installs) AS skan_installs
    		FROM
    			`{bq_dataset}.ios_campaign_skan_performance`
    		GROUP BY
    			1, 2
    	)
    	USING (day, campaign_id)
    LEFT JOIN 
    	ShareFinal
        USING(campaign_id, country_code)
    GROUP BY
    	 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
 );