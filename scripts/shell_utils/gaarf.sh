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
  gaarf $(dirname $0)/google_ads_queries/*/*.sql \
  --account=$customer_id \
  --output=bq \
  --customer-ids-query="$customer_ids_query" \
  --bq.project=$project --bq.dataset=$bq_dataset \
  --macro.start_date=$start_date --macro.end_date=$end_date \
  --ads-config=$ads_config "$@"
}

conversion_lag_adjustment() {
  $(which python3) $(dirname $0)/scripts/conv_lag_adjustment.py \
    --account=$customer_id --ads-config=$ads_config \
    --bq.project=$project --bq.dataset=$bq_dataset
}

backfill_snapshots() {
  $(which python3) $(dirname $0)/scripts/backfill_snapshots.py \
    --account=$customer_id --ads-config=$ads_config "$@"
}

fetch_video_orientation() {
  $(which python3) $(dirname $0)/scripts/fetch_video_orientation.py \
    --mode=$video_parsing_mode_output --account=$customer_id \
    -c=$config_file --ads-config=$ads_config \
    --project=$project --macro.bq_dataset=$bq_dataset --element-delimiter=$element_delimiter \
    --orientation-position=$orientation_position \
    --orientation-delimiter=$orientation_delimiter "$@"
}

generate_bq_views() {
  gaarf-bq $(dirname $0)/bq_queries/views_and_functions/*.sql \
    --project=$project --target=$bq_dataset $macros "$@"
}


generate_snapshots() {
  gaarf-bq $(dirname $0)/bq_queries/snapshots/*.sql \
    --project=$project --target=$bq_dataset $macros "$@"
}

generate_output_tables() {
  gaarf-bq $(dirname $0)/bq_queries/*.sql \
    --project=$project --target=$bq_dataset_output $macros "$@"
}

generate_legacy_views() {
  gaarf-bq $(dirname $0)/bq_queries/legacy_views/*.sql \
    --project=$project --target=$bq_dataset_legacy $macros "$@"
}

