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

-- Save asset performance data for a single day
CREATE OR REPLACE TABLE `{target_dataset}.asset_performance_{yesterday_iso}` AS
WITH Temp AS (
    SELECT
        *,
        MIN(day) OVER() AS min_day
    FROM `{target_dataset}.asset_performance_{date_iso}`
)
SELECT * EXCEPT (min_day) FROM temp WHERE day = min_day;

-- Save ad group network split data for a single day
CREATE OR REPLACE TABLE `{bq_dataset}.incremental_ad_group_network_split_{date_iso}` AS
WITH Temp AS (
    SELECT
        *,
        MIN(day) OVER() AS min_day
    FROM `{target_dataset}.ad_group_network_split`
)
SELECT * EXCEPT (min_day) FROM temp WHERE day = min_day;

-- Define view combining saved historical ad group network split data
-- with the latest fetched data
CREATE OR REPLACE VIEW `{target_dataset}.full_ad_group_network_split_view` AS
WITH Temp AS (
    SELECT
        *,
        MIN(day) OVER() AS min_day
    FROM `{target_dataset}.ad_group_network_split`
)
SELECT * FROM `{bq_dataset}.incremental_ad_group_network_split_*`
UNION ALL
SELECT * EXCEPT (min_day) FROM temp WHERE day > min_day;


-- Save geo performance data for a single day
CREATE OR REPLACE TABLE `{bq_dataset}.incremental_geo_performance_{date_iso}` AS
WITH Temp AS (
    SELECT
        *,
        MIN(day) OVER() AS min_day
    FROM `{target_dataset}.geo_performance`
)
SELECT * EXCEPT (min_day) FROM temp WHERE day = min_day;

-- Define view combining saved historical geo performance data
-- with the latest fetched data
CREATE OR REPLACE VIEW `{target_dataset}.full_geo_performance_view` AS
WITH Temp AS (
    SELECT
        *,
        MIN(day) OVER() AS min_day
    FROM `{target_dataset}.geo_performance`
)
SELECT * FROM `{bq_dataset}.incremental_geo_performance_*`
UNION ALL
SELECT * EXCEPT (min_day) FROM temp WHERE day > min_day;
