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

--TODO@dbenger: Adjust the script so that it resembles generic change_history script
CREATE TEMP FUNCTION GetCohort(arr ARRAY<FLOAT64>, day INT64)
RETURNS FLOAT64 AS (
    arr[SAFE_OFFSET(day)]
);
--TODO: When Conv. Lag Calculation is added, include it to this part
-- iOS Campaign Overall Performance with Cohorts
CREATE OR REPLACE TABLE {target_dataset}.ios_performance
AS (
  WITH 
  	ConversionsTable AS 
  	(
      SELECT --TODO: replace ad_group_id with campaing_id
          date,
          campaign_id,
          network,
          SUM(conversions) AS conversions,
          SUM(IF(conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
          SUM(IF(conversion_category != "DOWNLOAD", conversions, 0)) AS inapps
      FROM 
      	`{bq_dataset}.ios_campaign_conversion_split`
      GROUP BY 
      	1, 2, 3
  ),
  	iOSCampaignRaw AS
  	(
  		SELECT
			PARSE_DATE("%Y-%m-%d", IOP.date) AS day,
  			IOP.campaign_id,
  			SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            SUM(engagements) AS engagements,
            SUM(interactions) AS interactions,
            SUM(video_views) AS video_views,
            SUM(cost) AS cost,
            SUM(view_through_conversions) AS view_through_conversions,
            SUM(conversions) AS conversions,
            SUM(installs) AS installs,
            SUM(inapps) AS inapps,
            SUM(conversions_value) AS conversions_value,
            SUM(video_quartile_p25_rate) AS video_quartile_p25_rate,
            SUM(video_quartile_p50_rate) AS video_quartile_p50_rate,
            SUM(video_quartile_p75_rate) AS video_quartile_p75_rate,
            SUM(video_quartile_p100_rate) AS video_quartile_p100_rate,
            SUM(skan_installs) AS skan_installs
       	FROM
       		`{bq_dataset}.ios_campaign_overall_perfomance` AS IOP
       	LEFT JOIN
       		ConversionsTable AS CT
       		USING(date, campaign_id, network)
        LEFT JOIN
        (
        	SELECT
        		date,
        		campaign_id,
        		skan_installs
        	FROM
  				`{bq_dataset}.ios_campaign_skan_perfomance`
		) AS SP
			USING(date, campaign_id)
		 GROUP BY 
      	 	1, 2
  	),
	 iOSCampaignMapping AS
    (
      SELECT 
          campaign_id,
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
         ICR.day,
         ICR.campaign_id,
         M.account_id,
         M.account_name,
         M.currency,
         M.campaign_name,
         M.campaign_status,
         ACS.campaign_sub_type,
         IFNULL(G.geos, "All") AS geos,
         IFNULL(G.languages, "All") AS languages,
         ACS.app_id,
         ACS.app_store,
       	 ACS.bidding_strategy,
         ACS.target_conversions,
         ACS.start_date AS start_date,
         DATE_DIFF(
           CURRENT_DATE(),
           PARSE_DATE("%Y-%m-%d", ACS.start_date),
           DAY) AS days_since_start_date,
         ACS.n_of_target_conversions,
         `{bq_dataset}.NormalizeMillis`(B.budget_amount) AS current_budget_amount,
         `{bq_dataset}.NormalizeMillis`(B.target_cpa) AS current_target_cpa,
         B.target_roas AS current_target_roas,
         `{bq_dataset}.NormalizeMillis`(BidBudgetHistory.budget_amount) AS budget,
         `{bq_dataset}.NormalizeMillis`(BidBudgetHistory.target_cpa) AS target_cpa,
         BidBudgetHistory.target_roas AS target_roas,
         -- If cost for a given day is higher than allocated budget than is_budget_limited = 1
         IF(ICR.cost > BidBudgetHistory.budget_amount, 1, 0) AS is_budget_limited,
         -- If cost for a given day is 20% higher than allocated budget than is_budget_overshooting = 1
         IF(ICR.cost > BidBudgetHistory.budget_amount * 1.2, 1, 0) AS is_budget_overshooting,
         -- If cost for a given day is 50% lower than allocated budget than is_budget_underspend = 1
         IF(ICR.cost < BidBudgetHistory.budget_amount * 0.5, 1, 0) AS is_budget_underspend,
         -- If CPA for a given day is 10% higher than target_cpa than is_cpa_overshooting = 1
         IF(SAFE_DIVIDE(ICR.cost, ICR.conversions) > BidBudgetHistory.target_cpa * 1.1, 1, 0) AS is_cpa_overshooting,
         CASE
           WHEN BidBudgetHistory.target_cpa = LAG(BidBudgetHistory.target_cpa)
             OVER (PARTITION BY ICR.campaign_id ORDER BY ICR.day ASC)
           OR LAG(BidBudgetHistory.target_cpa)
             OVER (PARTITION BY ICR.campaign_id ORDER BY ICR.day ASC) IS NULL
           THEN 0
         ELSE 1
         END AS bid_changes,
         CASE
           WHEN BidBudgetHistory.budget_amount = LAG(BidBudgetHistory.budget_amount)
             OVER (PARTITION BY ICR.campaign_id ORDER BY ICR.day ASC)
           OR LAG(BidBudgetHistory.budget_amount)
             OVER (PARTITION BY ICR.campaign_id ORDER BY ICR.day ASC) IS NULL
           THEN 0
         ELSE 1
         END AS budget_changes,
         ICR.clicks,
         ICR.impressions,
         ICR.engagements,
         ICR.interactions,
         ICR.video_views,
         `{bq_dataset}.NormalizeMillis`(ICR.cost) AS cost,
         ICR.conversions,
         ICR.installs,
         ICR.inapps,
         ICR.conversions_value,
         ICR.view_through_conversions,
         ICR.video_quartile_p25_rate,
         ICR.video_quartile_p50_rate,
         ICR.video_quartile_p75_rate,
         ICR.video_quartile_p100_rate,
         ICR.skan_installs
     FROM
     	iOSCampaignRaw AS ICR
     LEFT JOIN
     	iOSCampaignMapping AS M
     	USING (campaign_id)
     LEFT JOIN 
     	{bq_dataset}.bids_budgets AS B
     	USING (campaign_id)
     LEFT JOIN 
     	`{bq_dataset}.bid_budgets_*` AS BidBudgetHistory
     	USING (day, campaign_id)	
     LEFT JOIN 
     	`{bq_dataset}.AppCampaignSettingsView` AS ACS
       	  USING (campaign_id)
     LEFT JOIN 
     	`{bq_dataset}.GeoLanguageView` AS G
       	 USING (campaign_id)
   ),
    
  iOSCampaignPerformance AS
    (
      SELECT
          day,
          account_id,
          account_name,
          campaign_id,
          campaign_name,
          campaign_status,
          currency,
          campaign_sub_type,
          geos,
          languages,
          app_id,
          app_store,
          bidding_strategy,
          "" AS firebase_bidding_status,
          target_conversions,
          start_date,
          days_since_start_date,
          n_of_target_conversions,
          MIN(current_budget_amount) AS current_budget_amount,
          MIN(current_target_cpa) AS current_target_cpa,
          MIN(current_target_roas) AS current_target_roas,
          MIN(budget) AS budget,
          MIN(target_cpa) AS target_cpa,
          MIN(target_roas) AS target_roas,
          MIN(is_budget_limited) AS is_budget_limited,
          MIN(is_budget_overshooting) AS is_budget_overshooting,
          MIN(is_budget_underspend) AS is_budget_underspend,
          MIN(is_cpa_overshooting) AS is_cpa_overshooting,
          MIN(bid_changes) AS bid_changes,
          MIN(budget_changes) AS budget_changes,
          MIN(clicks) AS clicks,
          MIN(impressions) AS impressions,
          MIN(cost) AS cost,
          MIN(installs) AS installs,
          MIN(inapps) AS inapps,
          MIN(video_views) AS video_views,
          MIN(interactions) AS interactions,
          MIN(engagements) AS engagements,
          MIN(conversions_value) AS conversions_value,
          MIN(view_through_conversions) AS view_through_conversions,
          MIN(video_quartile_p25_rate) AS video_quartile_p25_rate,
          MIN(video_quartile_p50_rate) AS video_quartile_p50_rate,
          MIN(video_quartile_p75_rate) AS video_quartile_p75_rate,
          MIN(video_quartile_p100_rate) AS video_quartile_p100_rate,
          MIN(skan_installs) AS skan_installs,
          CASE
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 0 AND 0.01 THEN '<1%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 0.01 AND 0.1 THEN '1%-10%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 0.1 AND 0.25 THEN '10%-25%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 0.25 AND 0.5 THEN '25%-50%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 0.5 AND 0.75 THEN '50%-75%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 0.75 AND 0.9 THEN '75%-90%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 0.9 AND 1 THEN '90%-100%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1 AND 1.10 THEN '100%-110%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1.10 AND 1.20 THEN '110%-120%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1.20 AND 1.30 THEN '120%-130%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1.30 AND 1.40 THEN '130%-140%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1.40 AND 1.50 THEN '140%-150%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1.50 AND 1.60 THEN '150%-160%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1.60 AND 1.70 THEN '160%-170%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1.70 AND 1.80 THEN '170%-180%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1.80 AND 1.90 THEN '180%-190%'
            WHEN SAFE_DIVIDE(MIN(cost),  MIN(budget)) BETWEEN 1.90 AND 2 THEN '190%-200%'
            ELSE '>200%'
          END AS budget_util_tier,
          CASE
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 0 AND 0.01 THEN '<1%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 0.01 AND 0.1 THEN '1%-10%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 0.1 AND 0.25 THEN '10%-25%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 0.25 AND 0.5 THEN '25%-50%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 0.5 AND 0.75 THEN '50%-75%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 0.75 AND 0.9 THEN '75%-90%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 0.9 AND 1 THEN '90%-100%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1 AND 1.10 THEN '100%-110%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1.10 AND 1.20 THEN '110%-120%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1.20 AND 1.30 THEN '120%-130%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1.30 AND 1.40 THEN '130%-140%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1.40 AND 1.50 THEN '140%-150%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1.50 AND 1.60 THEN '150%-160%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1.60 AND 1.70 THEN '160%-170%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1.70 AND 1.80 THEN '170%-180%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1.80 AND 1.90 THEN '180%-190%'
            WHEN bidding_strategy = "tCPA (AC Actions)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(cost), SUM(inapps)), AVG(target_cpa)) BETWEEN 1.90 AND 2 THEN '190%-200%'
            ELSE '>200%'
          END AS tcpa_bid_util_tier,
          CASE
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 0 AND 0.01 THEN '<1%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 0.01 AND 0.1 THEN '1%-10%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 0.1 AND 0.25 THEN '10%-25%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 0.25 AND 0.5 THEN '25%-50%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 0.5 AND 0.75 THEN '50%-75%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 0.75 AND 0.9 THEN '75%-90%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 0.9 AND 1 THEN '90%-100%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1 AND 1.10 THEN '100%-110%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1.10 AND 1.20 THEN '110%-120%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1.20 AND 1.30 THEN '120%-130%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1.30 AND 1.40 THEN '130%-140%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1.40 AND 1.50 THEN '140%-150%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1.50 AND 1.60 THEN '150%-160%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1.60 AND 1.70 THEN '160%-170%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1.70 AND 1.80 THEN '170%-180%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1.80 AND 1.90 THEN '180%-190%'
            WHEN bidding_strategy = "tROAS (AC ROAS)" AND SAFE_DIVIDE(SAFE_DIVIDE(SUM(conversions_value), SUM(cost)), AVG(target_roas)) BETWEEN 1.90 AND 2 THEN '190%-200%'
            ELSE '>200%'
          END AS troas_bid_util_tier
      FROM 
      	FinalTablePrep
      GROUP BY 
      	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
    )
    
SELECT
    day,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_status,
    currency,
    campaign_sub_type,
    geos,
    languages,
    app_id,
    app_store,
    bidding_strategy,
    target_conversions,
    start_date,
    days_since_start_date,
    n_of_target_conversions,
    CAST(budget_util_tier AS STRING) AS budget_util_tier ,
    CAST(troas_bid_util_tier AS STRING) AS troas_bid_util_tier,
    CAST(tcpa_bid_util_tier AS STRING) AS tcpa_bid_util_tier,
    SUM(current_budget_amount) AS current_budget_amount,
    SUM(current_target_cpa) AS current_target_cpa,
    SUM(current_target_roas) AS current_target_roas,
    SUM(budget) AS budget,
    SUM(target_cpa) AS target_cpa,
    SUM(target_roas) AS target_roas,
    SUM(is_budget_limited) AS is_budget_limited,
    SUM(is_budget_overshooting) AS is_budget_overshooting,
    SUM(is_budget_underspend) AS is_budget_underspend,
    SUM(is_cpa_overshooting) AS is_cpa_overshooting,
    SUM(bid_changes) AS bid_changes,
    SUM(budget_changes) AS budget_changes,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(cost) AS cost,
    SUM(installs) AS installs,
    SUM(inapps) AS inapps,
    SUM(video_views) AS video_views,
    SUM(interactions) AS interactions,
    SUM(engagements) AS engagements,
    SUM(conversions_value) AS conversions_value,
    SUM(view_through_conversions) AS view_through_conversions,
    SUM(video_quartile_p25_rate) AS video_quartile_p25_rate,
    SUM(video_quartile_p50_rate) AS video_quartile_p50_rate,
    SUM(video_quartile_p75_rate) AS video_quartile_p75_rate,
    SUM(video_quartile_p100_rate) AS video_quartile_p100_rate,
    SUM(skan_installs) AS skan_installs,
    {% for day in cohort_days %}
        -- SUM(GetCohort(CC.lag_data.skan_postbacks, {{day}})) AS skan_postbacks_{{day}}_day,
        SUM(GetCohort(CC.lag_data.installs, {{day}})) AS installs_{{day}}_day,
        SUM(GetCohort(CC.lag_data.inapps, {{day}})) AS inapps_{{day}}_day,
        SUM(GetCohort(CC.lag_data.conversions_value, {{day}})) AS conversions_value_{{day}}_day,
    {% endfor %}
FROM
    iOSCampaignPerformance AS ICP
LEFT JOIN 
(
	SELECT
		day_of_interaction AS day,
		campaign_id,
		lag_data
	FROM
		`{bq_dataset}.CampaignAdGroupCohorts`	
) AS CC
	USING (day, campaign_id)
GROUP BY 
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
);

# Copyright 2022 Google LLC
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

CREATE TEMPORARY FUNCTION equalsArr(x ARRAY<STRING>, y ARRAY<STRING>)
RETURNS INT64
LANGUAGE js AS r"""
var count = 0;
if(x.length >= y.length) {{
for(var i=0; i<x.length; i++)
   if(!x.includes(y[i])) {{
   count++;
   }}
}} else {{
for(var i=0; i<y.length; i++)
   if(!y.includes(x[i])) {{
   count++;
   }}
}}
return count
""";

-- Contains daily changes (bids, budgets, assets, etc) on campaign level.
CREATE OR REPLACE TABLE {target_dataset}.change_history
AS (
    WITH ConversionsTable AS (
        SELECT
            ConvSplit.date,
            ConvSplit.network,
            ConvSplit.ad_group_id,
            SUM(ConvSplit.conversions) AS conversions,
            SUM(IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0)) AS installs,
            SUM(
                IF(LagAdjustments.lag_adjustment IS NULL,
                    IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0),
                    ROUND(IF(ConvSplit.conversion_category = "DOWNLOAD", conversions, 0) / LagAdjustments.lag_adjustment))
            ) AS installs_adjusted,
            SUM(IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0)) AS inapps,
            SUM(
                IF(LagAdjustments.lag_adjustment IS NULL,
                    IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0),
                    ROUND(IF(ConvSplit.conversion_category != "DOWNLOAD", conversions, 0) / LagAdjustments.lag_adjustment))
            ) AS inapps_adjusted
        FROM `{bq_dataset}.ad_group_conversion_split` AS ConvSplit
        LEFT JOIN `{bq_dataset}.ConversionLagAdjustments` AS LagAdjustments
            ON PARSE_DATE("%Y-%m-%d", ConvSplit.date) = LagAdjustments.adjustment_date
                AND ConvSplit.network = LagAdjustments.network
                AND ConvSplit.conversion_id = LagAdjustments.conversion_id
        GROUP BY 1, 2, 3
    ),
    CampaignPerformance AS (
        SELECT
            PARSE_DATE("%Y-%m-%d", date) AS day,
            M.campaign_id,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            SUM(cost) AS cost,
            SUM(conversions) AS conversions,
            SUM(installs) AS installs,
            SUM(installs_adjusted) AS installs_adjusted,
            SUM(inapps) AS inapps,
            SUM(inapps_adjusted) AS inapps_adjusted,
            SUM(view_through_conversions) AS view_through_conversions,
            SUM(AP.conversions_value) AS conversions_value,
            SUM(skan_installs) AS skan_installs
        FROM `{bq_dataset}.ad_group_performance` AS AP
        LEFT JOIN ConversionsTable AS ConvSplit
            USING(date, ad_group_id, network)
        LEFT JOIN `{bq_dataset}.account_campaign_ad_group_mapping` AS M
            USING(ad_group_id)
        LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
          ON M.campaign_id = ACS.campaign_id
        LEFT JOIN
        (
            SELECT
        		date,
        		campaign_id,
        		skan_installs
        	FROM
  				`{bq_dataset}.ios_campaign_skan_perfomance`
		) AS SI
			USING(date, campaign_id)
        GROUP BY 1, 2
    ),
    CampaignMapping AS (
        SELECT DISTINCT
            account_id,
            account_name,
            currency,
            campaign_id,
            campaign_name,
            campaign_status
        FROM `{bq_dataset}.account_campaign_ad_group_mapping`
    ) ,
    AssetStructureDailyArrays AS (
       SELECT
            day,
            ad_group_id,
            SPLIT(install_headlines, "|") AS install_headlines,
            SPLIT(install_descriptions, "|") AS install_descriptions,
            SPLIT(engagement_headlines, "|") AS engagement_headlines,
            SPLIT(engagement_descriptions, "|") AS engagement_descriptions,
            SPLIT(pre_registration_headlines, "|") AS pre_registration_headlines,
            SPLIT(pre_registration_descriptions, "|") AS pre_registration_descriptions,
            SPLIT(install_images, "|") AS install_images,
            SPLIT(install_videos, "|") AS install_videos,
            SPLIT(engagement_images, "|") AS engagement_images,
            SPLIT(engagement_videos, "|") AS engagement_videos,
            SPLIT(pre_registration_images, "|") AS pre_registration_images,
            SPLIT(pre_registration_videos, "|") AS pre_registration_videos,
            SPLIT(install_media_bundles, "|") AS install_media_bundles
        FROM `{bq_dataset}.asset_structure_snapshot_*`
    ),
    AssetChangesRaw AS (
        SELECT
        day,
        ad_group_id,
        -- installs
        CASE
            WHEN LAG(install_headlines) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(install_headlines, LAG(install_headlines) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_headlines_changes,
        CASE
            WHEN LAG(install_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(install_descriptions, LAG(install_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_descriptions_changes,

        CASE
            WHEN LAG(install_videos) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(install_videos, LAG(install_videos) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_videos_changes,
        CASE
            WHEN LAG(install_images) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(install_images, LAG(install_images) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_images_changes,
        CASE
            WHEN LAG(install_media_bundles) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(install_media_bundles, LAG(install_media_bundles) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_media_bundles_changes,
        -- engagements
        CASE
            WHEN LAG(engagement_headlines) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(engagement_headlines, LAG(engagement_headlines) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS engagement_headlines_changes,
        CASE
            WHEN LAG(engagement_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(engagement_descriptions, LAG(engagement_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS engagement_descriptions_changes,
        CASE
            WHEN LAG(engagement_videos) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(engagement_videos, LAG(engagement_videos) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS engagement_videos_changes,
        CASE
            WHEN LAG(engagement_images) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(engagement_images, LAG(engagement_images) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS engagement_images_changes,
        CASE
            WHEN LAG(pre_registration_headlines) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(pre_registration_headlines, LAG(pre_registration_headlines) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS pre_registration_headlines_changes,
        CASE
            WHEN LAG(pre_registration_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(pre_registration_descriptions, LAG(pre_registration_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS pre_registration_descriptions_changes,
        -- pre_registrations
        CASE
            WHEN LAG(pre_registration_videos) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(pre_registration_videos, LAG(pre_registration_videos) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS pre_registration_videos_changes,
        CASE
            WHEN LAG(pre_registration_images) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE equalsArr(pre_registration_images, LAG(pre_registration_images) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS pre_registration_images_changes
        FROM AssetStructureDailyArrays
    ),
    AssetChanges AS (
        SELECT
            day,
            campaign_id,
            SUM(install_headlines_changes) + SUM(engagement_headlines_changes) + SUM(pre_registration_headlines_changes) AS headline_changes,
            SUM(install_descriptions_changes) + SUM(engagement_descriptions_changes) + SUM(pre_registration_descriptions_changes) AS description_changes,
            SUM(install_images_changes) + SUM(engagement_images_changes) + SUM(pre_registration_images_changes) AS image_changes,
            SUM(install_videos_changes) + SUM(engagement_videos_changes) + SUM(pre_registration_videos_changes) AS video_changes,
            SUM(install_media_bundles_changes) AS html5_changes,
        FROM AssetChangesRaw
        LEFT JOIN `{bq_dataset}.account_campaign_ad_group_mapping`
            USING(ad_group_id)
        GROUP BY 1, 2
    )
SELECT
    CP.day,
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
    ACS.start_date AS start_date,
    DATE_DIFF(
        CURRENT_DATE(),
        PARSE_DATE("%Y-%m-%d", ACS.start_date),
        DAY) AS days_since_start_date,
    ACS.n_of_target_conversions,
    `{bq_dataset}.NormalizeMillis`(B.budget_amount) AS current_budget_amount,
    `{bq_dataset}.NormalizeMillis`(B.target_cpa) AS current_target_cpa,
    B.target_roas AS current_target_roas,
    `{bq_dataset}.NormalizeMillis`(BidBudgetHistory.budget_amount) AS budget,
    `{bq_dataset}.NormalizeMillis`(BidBudgetHistory.target_cpa) AS target_cpa,
    BidBudgetHistory.target_roas AS target_roas,
    -- If cost for a given day is higher than allocated budget than is_budget_limited = 1
    IF(CP.cost > BidBudgetHistory.budget_amount, 1, 0) AS is_budget_limited,
    -- If cost for a given day is 20% higher than allocated budget than is_budget_overshooting = 1
    IF(CP.cost > BidBudgetHistory.budget_amount * 1.2, 1, 0) AS is_budget_overshooting,
    -- If cost for a given day is 50% lower than allocated budget than is_budget_underspend = 1
    IF(CP.cost < BidBudgetHistory.budget_amount * 0.5, 1, 0) AS is_budget_underspend,
    -- If CPA for a given day is 10% higher than target_cpa than is_cpa_overshooting = 1
    IF(SAFE_DIVIDE(CP.cost, CP.conversions) > BidBudgetHistory.target_cpa * 1.1, 1, 0) AS is_cpa_overshooting,
    AssetChanges.headline_changes + AssetChanges.description_changes AS text_changes,
    AssetChanges.image_changes AS image_changes,
    AssetChanges.html5_changes AS html5_changes,
    AssetChanges.video_changes AS video_changes,
    0 AS geo_changes,
    0 AS ad_group_added,
    0 AS ad_group_resumed,
    0 AS ad_group_paused,
    0 AS ad_group_deleted,
    CASE
        WHEN BidBudgetHistory.target_cpa = LAG(BidBudgetHistory.target_cpa)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC)
        OR LAG(BidBudgetHistory.target_cpa)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC) IS NULL
        THEN 0
    ELSE 1
    END AS bid_changes,
    CASE
        WHEN BidBudgetHistory.budget_amount = LAG(BidBudgetHistory.budget_amount)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC)
        OR LAG(BidBudgetHistory.budget_amount)
            OVER (PARTITION BY M.campaign_id ORDER BY CP.day ASC) IS NULL
        THEN 0
    ELSE 1
    END AS budget_changes,
    CP.clicks,
    CP.impressions,
    `{bq_dataset}.NormalizeMillis`(CP.cost) AS cost,
    IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
        CP.installs, CP.inapps) AS conversions,
    IF(ACS.bidding_strategy = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
        CP.installs_adjusted, CP.inapps_adjusted) AS conversions_adjusted,
    CP.installs,
    CP.installs_adjusted,
    CP.inapps,
    CP.inapps_adjusted,
    CP.view_through_conversions,
    CP.conversions_value
    CP.skan_installs
FROM CampaignPerformance AS CP
LEFT JOIN CampaignMapping AS M
    ON CP.campaign_id = M.campaign_id
LEFT JOIN `{bq_dataset}.bid_budget` AS B
    ON CP.campaign_id = B.campaign_id
LEFT JOIN `{bq_dataset}.bid_budgets_*` AS BidBudgetHistory
    ON M.campaign_id = BidBudgetHistory.campaign_id
        AND CP.day = BidBudgetHistory.day
LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
LEFT JOIN AssetChanges
    ON M.campaign_id = AssetChanges.campaign_id
        AND CP.day = AssetChanges.day);
