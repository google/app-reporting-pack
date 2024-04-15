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

-- Contains daily changes (bids, budgets, assets, etc) on campaign level.
CREATE OR REPLACE TABLE `{target_dataset}.change_history`
AS (
  WITH SkanPostbacksTable AS (
    SELECT
      PARSE_DATE("%Y-%m-%d", CAST(date AS STRING)) AS day,
      campaign_id,
      SUM(skan_postbacks) AS skan_postbacks
    FROM `{bq_dataset}.ios_campaign_skan_performance`
    GROUP BY 1, 2
  )
  SELECT
    CP.*,
    IFNULL(S.skan_postbacks, 0) AS skan_postbacks
  FROM `{target_dataset}.change_history` AS CP
  LEFT JOIN SkanPostbacksTable AS S
    ON CP.day = S.day AND CP.campaign_id = S.campaign_id
);
