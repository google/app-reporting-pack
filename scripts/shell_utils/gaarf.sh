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

fetch_skan_reports() {
  skan_customer_ids_query='SELECT customer.id FROM campaign WHERE campaign.advertising_channel_type = "MULTI_CHANNEL" AND campaign.app_campaign_setting.app_store = "APPLE_APP_STORE"'
  gaarf $(dirname $0)/ios_skan/google_ads_queries/*.sql \
  --account=$customer_id \
  --output=bq \
  --customer-ids-query="$skan_customer_ids_query" \
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
    --youtube-config-path=$youtube_config_path \
    --project=$project --macro.bq_dataset=$bq_dataset \
    --element-delimiter=$element_delimiter \
    --orientation-position=$orientation_position \
    --orientation-delimiter=$orientation_delimiter "$@"
}

create_skan_schema() {
  $(which python3) $(dirname $0)/scripts/create_skan_schema.py \
    -m=$skan_schema_mode \
    -c=$config_file \
    --project=$project --macro.skan_schema_input_table=$skan_schema_input_table \
    $macros "$@"
}

generate_bq_views() {
  gaarf-bq $(dirname $0)/bq_queries/views_and_functions/*.sql \
    --project=$project $macros "$@"
}


generate_snapshots() {
  gaarf-bq $(dirname $0)/bq_queries/snapshots/*.sql \
    --project=$project $macros "$@"
}

generate_output_tables() {
  gaarf-bq $(dirname $0)/bq_queries/*.sql \
    --project=$project --target=$bq_dataset_output $macros "$@"
}

generate_legacy_views() {
  gaarf-bq $(dirname $0)/bq_queries/legacy_views/*.sql \
    --project=$project $macros "$@"
}

generate_skan_output_tables() {
  gaarf-bq $(dirname $0)/ios_skan/bq_queries/*.sql \
    --project=$project $macros "$@"
}

save_initial() {
  gaarf-bq $(dirname $0)/bq_queries/incremental/initial_run/*.sql \
    --project=$project $initial_run_macros "$@"
}

save_incremental() {
  gaarf-bq $(dirname $0)/bq_queries/incremental/*.sql \
    --project=$project $macros "$@"
}
