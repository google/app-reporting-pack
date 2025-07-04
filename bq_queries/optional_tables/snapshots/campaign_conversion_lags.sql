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

-- All App Campaigns | Contains conversions and their values on a given day with automatically calculated lag number
CREATE OR REPLACE TABLE {bq_dataset}.campaign_conversion_lags_{date_iso}
AS (
	SELECT
	    PARSE_DATE("%Y-%m-%d", AGP.date) AS day_of_interaction,
	    DATE_DIFF(
	        CURRENT_DATE(),
	        PARSE_DATE("%Y-%m-%d", AGP.date),
	        DAY) AS lag,
	    campaign_id,
	    ad_group_id,
	    network,
	    SUM(IF(CS.conversion_category = "DOWNLOAD", CS.conversions, 0)) AS installs,
	    SUM(IF(CS.conversion_category != "DOWNLOAD", CS.conversions, 0)) AS inapps,
	    SUM(AGP.view_through_conversions) AS view_through_conversions,
	    SUM(AGP.conversions_value) AS conversions_value
	FROM 
		{bq_dataset}.ad_group_performance AS AGP
	LEFT JOIN
		{bq_dataset}.ad_group_conversion_split AS CS
	    USING(date, ad_group_id, campaign_id, network)
	GROUP BY 
		1, 2, 3, 4, 5
	);