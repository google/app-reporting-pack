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

-- Overall App Campaigns | Aggregated Daily Campaign & AdGroup Conversion Lag Snapshots
CREATE OR REPLACE VIEW `{bq_dataset}.CampaignAdGroupCohorts` AS (
WITH
	RawCampaignAdGroupLags AS
		(
			SELECT 
                * 
            FROM 
                `{bq_dataset}.campaign_conversion_lags_*`
		)
		
    SELECT
        day_of_interaction,
        campaign_id,
        ad_group_id,
        network,
        STRUCT(
            ARRAY_AGG(lag ORDER BY lag) AS lags,
            ARRAY_AGG(installs ORDER BY lag) AS installs,
            ARRAY_AGG(inapps ORDER BY lag) AS inapps,
            ARRAY_AGG(conversions_value ORDER BY lag) AS conversions_value,
            ARRAY_AGG(view_through_conversions ORDER BY lag) AS view_through_conversions
        ) AS lag_data
    FROM 
    	RawCampaignAdGroupLags
    WHERE
        day_of_interaction IS NOT NULL
        AND lag <= 90
    GROUP BY 
    	1, 2, 3, 4
);