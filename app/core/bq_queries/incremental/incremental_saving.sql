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

-- Save ad_group performance data for a single day
CREATE OR REPLACE TABLE `{target_dataset}.ad_group_network_split_{yesterday_iso}` AS
SELECT * FROM `{target_dataset}.ad_group_network_split_{date_iso}`
WHERE day <= "{start_date}";

-- Save ad_group performance data for the current fetch
CREATE OR REPLACE TABLE `{target_dataset}.ad_group_network_split_{date_iso}` AS
SELECT * FROM `{target_dataset}.ad_group_network_split_{date_iso}`
WHERE day > "{start_date}";

