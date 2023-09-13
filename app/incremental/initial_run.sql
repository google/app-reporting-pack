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

-- Save asset performance data till some cutoff date
CREATE OR REPLACE TABLE `{bq_dataset}.incremental_asset_performance_{initial_date}` AS
SELECT
    *
FROM `{target_dataset}.asset_performance`
WHERE day < "{cutoff_date}";

-- Save ad group network split data till some cutoff date
CREATE OR REPLACE TABLE `{bq_dataset}.incremental_ad_group_network_split_{initial_date}` AS
SELECT
    *
FROM `{target_dataset}.ad_group_network_split`
WHERE day < "{cutoff_date}";

-- Save geo performance data till some cutoff date
CREATE OR REPLACE TABLE `{bq_dataset}.incremental_geo_performance_{initial_date}` AS
SELECT
    *
FROM `{target_dataset}.geo_performance`
WHERE day < "{cutoff_date}";
