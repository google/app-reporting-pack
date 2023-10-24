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

-- Save geo performance data till some cutoff date
CREATE OR REPLACE TABLE `{target_dataset}.geo_performance_{initial_date}` AS
SELECT * FROM `{target_dataset}.geo_performance_*`
WHERE day < "{start_date}";

CREATE OR REPLACE TABLE `{target_dataset}.geo_performance_{date_iso}` AS
SELECT * FROM `{target_dataset}.geo_performance_*`
WHERE day >= "{start_date}";


