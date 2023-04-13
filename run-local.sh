#!/bin/bash
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

. ./scripts/shell_utils/app_reporting_pack.sh
. ./scripts/shell_utils/gaarf.sh
. ./scripts/shell_utils/functions.sh

set -e
COLOR='\033[0;36m' # Cyan
NC='\033[0m' # No color
usage="bash run-local.sh -c|--config <config> -q|--quiet\n\n
Helper script for running App Reporting Pack queries.\n\n
-h|--help - show this help message\n
-c|--config <config> - path to config.yaml file, i.e., path/to/app_reporting_pack.yaml\n
-q|--quiet - skips all confirmation prompts and starts running scripts based on config files\n
-g|--google-ads-config - path to google-ads.yaml file (by default it expects it in $HOME directory)\n
-l|--loglevel - loglevel (DEBUG, INFO, WARNING, ERROR), INFO by default.\n
--legacy - generates legacy views that can be plugin into existing legacy dashboard.\n
--backfill - whether to perform backfill of the bid and budgets snapshots.\n
--backfill-only - perform only backfill of the bid and budgets snapshots.\n
--generate-config-only - perform only config generation instead of fetching data from Ads API.\n
"

solution_name="App Reporting Pack"
solution_name_lowercase=$(echo $solution_name | tr '[:upper:]' '[:lower:]' |\
  tr ' ' '_')

quiet="n"
generate_config_only="n"

while :; do
case $1 in
  -q|--quiet)
    quiet="y"
    ;;
  -c|--config)
    shift
    config_file=$1
    ;;
  -l|--loglevel)
    shift
    loglevel=$1
    ;;
  -g|--google-ads-config)
    shift
    google_ads_config=$1
    ;;
  --legacy)
    legacy="y"
    ;;
  --backfill)
    backfill="y"
    ;;
  --backfill-only)
    backfill_only="y"
    ;;
  --generate-config-only)
    generate_config_only="y"
    ;;
  -h|--help)
    echo -e $usage;
    exit
    ;;
  *)
    break
  esac
  shift
done

# Specify customer ids query that fetch data only from accounts that have at least one app campaign in them.
customer_ids_query='SELECT customer.id FROM campaign WHERE campaign.advertising_channel_type = "MULTI_CHANNEL"'
API_VERSION="12"

welcome() {
  echo -e "${COLOR}Welcome to installation of $solution_name${NC} "
}

setup() {
  # TODO: get default value from google-ads.yaml
  echo -n "Enter account_id in XXXXXXXXXX format: "
  read -r customer_id

  default_project=${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project 2>/dev/null)}
  echo -n "Enter BigQuery project_id ($default_project): "
  read -r project
  project=${project:-$default_project}

  echo -n "Enter BigQuery dataset (arp): "
  read -r bq_dataset
  bq_dataset=${bq_dataset:-arp}

  echo -n "Enter start_date in YYYY-MM-DD format (or use :YYYYMMDD-90 for last 90 days): "
  read -r start_date
  echo -n "Enter end_date in YYYY-MM-DD format (or use :YYYYMMDD-1 for yesterday): "
  read -r end_date
  start_date=${start_date:-:YYYYMMDD-90}
  end_date=${end_date:-:YYYYMMDD-1}

  ask_for_cohorts
  ask_for_video_orientation
  generate_bq_macros

  if [[ -n $RUNNING_IN_GCE && $generate_config_only ]]; then
    # if you're running inside Google Cloud Compute Engine as generating config 
    # (see gcp/cloud-run-button/main.sh) then there's no need for additional questions
    save_config="--save-config --config-destination=$solution_name_lowercase.yaml"
    echo -e "${COLOR}Saving configuration to $solution_name_lowercase.yaml${NC}"
    fetch_reports $save_config --log=$loglevel --api-version=$API_VERSION --dry-run
    generate_output_tables $save_config --log=$loglevel --dry-run
    fetch_video_orientation $save_config --log=$loglevel --dry-run
    exit
  fi

  echo -n "Do you want to save this config (Y/n): "
  read -r save_config_answer
  save_config_answer=$(convert_answer $save_config_answer 'Y')
  if [[ $save_config_answer = "y" ]]; then
    echo -n "Config will be saved to $solution_name_lowercase.yaml, do you want to save it here? Continue[Y] or Change[n]: "
    read -r config_change_answer
    config_change_answer=$(convert_answer $config_change_answer 'Y')
    if [[ $config_change_answer = "n" ]]; then
      echo -n "Enter name of the config (without .yaml file extension): "
      read -r solution_name_lowercase
    fi
    save_config="--save-config --config-destination=$solution_name_lowercase.yaml"
    echo -e "${COLOR}Saving configuration to $solution_name_lowercase.yaml${NC}"
    fetch_reports $save_config --log=$loglevel --api-version=$API_VERSION --dry-run
    generate_output_tables $save_config --log=$loglevel --dry-run
    fetch_video_orientation $save_config --log=$loglevel --dry-run
    if [[ $generate_config_only = "y" ]]; then
      exit
    fi
  elif [[ $save_config_answer = "q" ]]; then
    exit
  fi
  print_configuration
}

print_configuration() {
  echo "Your configuration:"
  echo "  account_id: $customer_id"
  echo "  BigQuery project_id: $project"
  echo "  BigQuery dataset:: $bq_dataset"
  echo "  Start date: $start_date"
  echo "  End date: $end_date"
  echo "  Ads config: $ads_config"
  echo "  Cohorts: $cohorts_final"
  echo "  Video parsing mode: $video_parsing_mode_output"
}

run_with_config() {
  echo -e "${COLOR}Running with $config_file${NC}"
  if [[ -f "$config_file" ]]; then
    cat $config_file
  fi
  if [[ $backfill_only = "y" ]]; then
    echo -e "${COLOR}===backfilling snapshots===${NC}"
      $(which python3) $(dirname $0)/scripts/backfill_snapshots.py \
        -c=$config_file \
        --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
      exit
  fi
  echo -e "${COLOR}===fetching reports===${NC}"
  gaarf $(dirname $0)/google_ads_queries/**/*.sql -c=$config_file \
    --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
  echo -e "${COLOR}===calculating conversion lag adjustment===${NC}"
  $(which python3) $(dirname $0)/scripts/conv_lag_adjustment.py \
    -c=$config_file \
    --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
  echo -e "${COLOR}===generating snapshots===${NC}"
  gaarf-bq $(dirname $0)/bq_queries/snapshots/*.sql -c=$config_file --log=$loglevel
  if [[ $backfill = "y" ]]; then
    echo -e "${COLOR}===backfilling snapshots===${NC}"
      $(which python3) $(dirname $0)/scripts/backfill_snapshots.py \
        -c=$config_file \
        --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
  fi
  echo -e "${COLOR}===generating views and functions===${NC}"
  gaarf-bq $(dirname $0)/bq_queries/views_and_functions/*.sql -c=$config_file --log=$loglevel
  echo -e "${COLOR}===getting video orientation===${NC}"
  $(which python3) $(dirname $0)/scripts/fetch_video_orientation.py \
    -c=$config_file \
    --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
  echo -e "${COLOR}===generating final tables===${NC}"
  gaarf-bq $(dirname $0)/bq_queries/*.sql -c=$config_file --log=$loglevel
  if [[ $legacy = "y" ]]; then
    echo -e "${COLOR}===generating legacy views===${NC}"
    gaarf-bq $(dirname $0)/bq_queries/legacy_views/*.sql -c=$config_file --log=$loglevel
  fi

}

run_with_parameters() {
  echo -e "${COLOR}===fetching reports===${NC}"
  fetch_reports $save_config --log=$loglevel --api-version=$API_VERSION
  echo -e "${COLOR}===calculating conversion lag adjustment===${NC}"
  conversion_lag_adjustment --customer--ids-query="$customer_ids_query" \
    --api-version=$API_VERSION
  echo -e "${COLOR}===generating snapshots===${NC}"
  generate_snapshots $save_config --log=$loglevel
  if [[ $backfill = "y" ]]; then
    echo -e "${COLOR}===backfilling snapshots===${NC}"
    backfill_snapshots --customer--ids-query="$customer_ids_query" \
      --api-version=$API_VERSION
  fi
  echo -e "${COLOR}===generating views and functions===${NC}"
  generate_bq_views $save_config --log=$loglevel
  echo -e "${COLOR}===getting video orientation===${NC}"
  fetch_video_orientation $save_config --log=$loglevel --api-version=$API_VERSION
  echo -e "${COLOR}===generating final tables===${NC}"
  generate_output_tables $save_config --log=$loglevel
  if [[ $legacy = "y" ]]; then
    echo -e "${COLOR}===generating legacy views===${NC}"
    generate_legacy_views $save_config --log=$loglevel
  fi
}

check_ads_config

if [[ -z ${loglevel} ]]; then
  loglevel="INFO"
fi

if [[ -n "$config_file" || -f $solution_name_lowercase.yaml ]]; then
  config_file=${config_file:-$solution_name_lowercase.yaml}
  if [[ $quiet = "y" ]]; then
    run_with_config
  else
    echo -e "${COLOR}Found saved configuration at $config_file${NC}"
    if [[ -f "$config_file" ]]; then
      cat $config_file
    fi
    echo -n -e "${COLOR}Do you want to use it (Y/n/q): ${NC}"
    read -r setup_config_answer
    setup_config_answer=$(convert_answer $setup_config_answer 'Y')
    if [[ $setup_config_answer = "y" ]]; then
      echo -e "${COLOR}Using saved configuration...${NC}"
      run_with_config
    elif [[ $setup_config_answer = "n" ]]; then
      echo -n -e "${COLOR}[C]hoose an existing configuration or [S]tart setup: (C/S): ${NC}"
      read -r new_config_start_over
      new_config_start_over=$(convert_answer $new_config_start_over)
      if [[ $new_config_start_over = "c" ]]; then
        echo -n -e "${COLOR}Provide full path to saved configuration: ${NC}"
        read -r config_file
        run_with_config
      elif [[ $new_config_start_over = "s" ]]; then
        setup
        prompt_running
        run_with_parameters
      else
        echo "Unknown command, exiting"
        exit 1
      fi
    else
      echo "Exiting"
      exit
    fi
  fi
else
  welcome
  setup
  prompt_running
  run_with_parameters
fi
