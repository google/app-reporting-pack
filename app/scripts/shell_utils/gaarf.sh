#!/bin/bash
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

fetch_reports() {
  gaarf $(dirname $0)/core/google_ads_queries/*/*.sql \
  --account=$customer_id \
  --output=bq \
  --customer-ids-query="$customer_ids_query" \
  --bq.project=$project --bq.dataset=$bq_dataset \
  --bq.location=$bq_dataset_location \
  --macro.start_date=$start_date --macro.end_date=$end_date \
  --ads-config=$ads_config "$@"
}

create_skan_schema() {
  $(which python3) $(dirname $0)/scripts/create_skan_schema.py \
    -m=$skan_schema_mode \
    -c=$config_file \
    --project=$project --macro.skan_schema_input_table=$skan_schema_input_table \
    $macros "$@"
}

generate_output_tables() {
  gaarf-bq $(dirname $0)/core/bq_queries/*.sql \
    --project=$project --dataset-location=$bq_dataset_location \
    $macros "$@"
}
