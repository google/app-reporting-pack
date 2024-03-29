-- Copyright 2023 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Contains Campaign & Ad Group level Conversion Lag performance segmented by network (Search, Display, YouTube).
-- Added Cohort Performance view on the Campaign and AgGroup Level
CREATE TEMP FUNCTION GetCohort(arr ARRAY<FLOAT64>, day INT64)
RETURNS FLOAT64
AS (
    arr[SAFE_OFFSET(day)]
);
CREATE OR REPLACE TABLE `{target_dataset}.conversion_lag_performance`
AS (
    WITH
    	ConversionsTable AS
    	(
        SELECT
            CL.date,
            CL.network,
            CL.ad_group_id,
            CASE
                WHEN CL.conversion_lag = 'LESS_THAN_ONE_DAY' THEN "< 1 Day"
                WHEN CL.conversion_lag = 'ONE_TO_TWO_DAYS' THEN "1-2 Days"
                WHEN CL.conversion_lag = 'TWO_TO_THREE_DAYS' THEN "2-3 Days"
                WHEN CL.conversion_lag = 'THREE_TO_FOUR_DAYS' THEN "3-4 Days"
                WHEN CL.conversion_lag = 'FOUR_TO_FIVE_DAYS' THEN "4-5 Days"
                WHEN CL.conversion_lag = 'FIVE_TO_SIX_DAYS'THEN "5-6 Days"
                WHEN CL.conversion_lag = 'SIX_TO_SEVEN_DAYS' THEN "6-7 Days"
                WHEN CL.conversion_lag = 'SEVEN_TO_EIGHT_DAYS' THEN "7-8 Days"
                WHEN CL.conversion_lag = 'EIGHT_TO_NINE_DAYS' THEN "8-9 Days"
                WHEN CL.conversion_lag = 'NINE_TO_TEN_DAYS' THEN "9-10 Days"
                WHEN CL.conversion_lag = 'TEN_TO_ELEVEN_DAYS' THEN "10-11 Days"
                WHEN CL.conversion_lag = 'ELEVEN_TO_TWELVE_DAYS' THEN "11-12 Days"
                WHEN CL.conversion_lag = 'TWELVE_TO_THIRTEEN_DAYS' THEN "12-13 Days"
                WHEN CL.conversion_lag = 'THIRTEEN_TO_FOURTEEN_DAYS' THEN "13-14 Days"
                WHEN CL.conversion_lag = 'FOURTEEN_TO_TWENTY_ONE_DAYS' THEN "14-21 Days"
                WHEN CL.conversion_lag = 'TWENTY_ONE_TO_THIRTY_DAYS' THEN "21-30 Days"
                WHEN CL.conversion_lag = 'THIRTY_TO_FORTY_FIVE_DAYS' THEN "30-45 Days"
                ELSE '45+ Days'
            END AS conversion_lag_range,
            CL.conversion_name,
            CL.conversion_id,
            CAS.third_party_app_analytics_event_name,
            CAS.third_party_app_analytics_provider_name,
            CAS.firebase_event_name,
            CAS.firebase_property_name,
            CAS.conversion_event_status,
            CAS.conversion_event_type,
            CAS.conversion_event_counting_type,
            CAS.include_event_in_conversions_metric,
            CAS.conversion_event_view_through_lookback_window,
            CAS.conversion_event_click_through_lookback_window,
            SUM(CL.all_conversions) AS all_conversions,
            SUM(CL.all_conversions_value) AS all_conversions_value,
            SUM(IF(CL.conversion_category = "DOWNLOAD", CL.all_conversions, 0)) AS all_installs,
            SUM(IF(CL.conversion_category != "DOWNLOAD", CL.all_conversions, 0)) AS all_inapps
        FROM
        	`{bq_dataset}.ad_group_conversion_lag` AS CL
       	LEFT JOIN
 			`{bq_dataset}.account_conversion_action_settings` AS CAS
 			ON CL.conversion_id = CAST(CAS.conversion_id AS STRING)
        GROUP BY
        	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
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
     )

	SELECT
	    PARSE_DATE("%Y-%m-%d", CT.date) AS day,
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
	    CT.network,
	    CT.conversion_name,
      CT.conversion_id,
      CT.conversion_lag_range,
      CT.third_party_app_analytics_event_name,
      CT.third_party_app_analytics_provider_name,
      CT.firebase_event_name,
      CT.firebase_property_name,
      CT.conversion_event_status,
      CT.conversion_event_type,
      CT.conversion_event_counting_type,
      CT.include_event_in_conversions_metric,
      CT.conversion_event_view_through_lookback_window,
      CT.conversion_event_click_through_lookback_window,
	    SUM(CT.all_conversions) AS all_conversions,
	    SUM(CT.all_installs) AS all_installs,
	    SUM(CT.all_inapps) AS all_inapps,
	    SUM(CT.all_conversions_value) AS all_conversions_value
	FROM
		ConversionsTable AS CT
	LEFT JOIN
		MappingTable AS M
		USING (ad_group_id)
	LEFT JOIN
		`{bq_dataset}.AppCampaignSettingsView` AS ACS
	 	ON M.campaign_id = ACS.campaign_id
	LEFT JOIN
		`{bq_dataset}.GeoLanguageView` AS G
	  	ON M.campaign_id =  G.campaign_id
	GROUP BY
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
		23, 24, 25, 26, 27, 28, 29, 30, 31, 32
    )
