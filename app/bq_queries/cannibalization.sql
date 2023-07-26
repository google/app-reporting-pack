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

CREATE TEMP FUNCTION GetHash(hash_string STRING)
RETURNS STRING
AS (TO_HEX(MD5(hash_string)));

CREATE OR REPLACE TABLE {target_dataset}.cannibalization
AS (
    SELECT
        campaign_id,
        GetHash(CONCAT(G.geos, G.languages, A.app_id, A.bidding_strategy, A.target_conversions)) AS hash_strict,
        GetHash(CONCAT(G.geos, G.languages, A.app_id, A.bidding_strategy)) AS hash_medium,
        GetHash(CONCAT(G.geos, G.languages, A.app_id)) AS hash_flex,
        GetHash(CONCAT(G.geos, G.languages)) AS hash_broad
    FROM `{bq_dataset}.AppCampaignSettingsView` AS A
    LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
        USING(campaign_id)
);
