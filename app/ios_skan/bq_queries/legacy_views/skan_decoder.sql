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

-- Contains campaign level iOS SKAN performance with decoded events from SKAN input schema
{% if has_skan == "true" %}
CREATE OR REPLACE VIEW `{legacy_dataset}.skan_decoder`
AS SELECT * except(app_id), app_id AS AppId FROM
    {% if incremental == "true" %}
        `{target_dataset}.skan_decoder_*`
    {% else %}
    `{target_dataset}.skan_decoder`;
    {% endif %}
{% else %}
SELECT FALSE;
{% endif %}


