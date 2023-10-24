-- Copyright 2022 Google LLC
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

-- Contains dynamics by ad_group approval status.
CREATE OR REPLACE TABLE `{bq_dataset}.ad_group_approval_statuses_{date_iso}`
AS (
SELECT
    CURRENT_DATE() AS day,
    A.ad_group_id,
    A.approval_status,
    A.review_status,
    A.policy_topic_type,
    A.policy_topics,
    "" AS evidences
FROM `{bq_dataset}.ad_group_ad_disapprovals` AS A
);
