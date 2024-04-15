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
SCRIPT_PATH=$(readlink -f "$0" | xargs dirname)

source $SCRIPT_PATH/scripts/shell_utils/app_reporting_pack.sh
source $SCRIPT_PATH/scripts/shell_utils/gaarf.sh
source $SCRIPT_PATH/scripts/shell_utils/functions.sh

set -e
RED='\033[0;31m'
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
--initial-load - perform initial load outside for start and end date window.\n
--generate-config-only - perform only config generation instead of fetching data from Ads API.\n
--modules - comma separated list of modules to run
"

solution_name="App Reporting Pack"
solution_name_lowercase=$(echo $solution_name | tr '[:upper:]' '[:lower:]' |\
  tr ' ' '_')

quiet="n"
generate_config_only="n"
validate_ads_config="n"
modules="core,assets,disapprovals,ios_skan,geo"
incremental="y"
backfill="y"

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
    ads_config=$1
    ;;
  --legacy)
    legacy="y"
    ;;
  --backfill)
    backfill="y"
    ;;
  --initial-load)
    initial_mode=0
    ;;
  --initial-load-start-date)
    initial_mode_date=$1
    ;;
  --backfill-only)
    backfill_only="y"
    ;;
  --generate-config-only)
    generate_config_only="y"
    ;;
  --validate-google-ads-config)
    validate_ads_config="y"
    ;;
  --modules)
    shift
    modules=$1
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
API_VERSION="14"
#
welcome() {
  echo -e "${COLOR}Welcome to installation of $solution_name${NC} "
  echo -e "${COLOR}Follow this link for detailed installation guide: https://github.com/google/app-reporting-pack/blob/main/docs/installation-guide.md${NC} "
  echo
  echo "The solution will be deployed with the following default values"
  print_configuration
  echo -n "Press n to change the configuration or Enter to continue: "
  read -r defaults
  defaults=$(convert_answer $defaults 'y')
  echo
}

setup() {
  # get default value from google-ads.yaml
  if [[ $defaults != "y" ]]; then
    echo "Please configure the solution. The default answers are specified in parentheses, press Enter to select them"
    if [[ -n $ads_config ]]; then
      parse_yaml $ads_config "GOOGLE_ADS_"
      local login_customer_id=$GOOGLE_ADS_login_customer_id
    fi
    echo -n "Enter account_id in XXXXXXXXXX format ($login_customer_id): "
    read -r customer_id
    customer_id=${customer_id:-$login_customer_id}


    gcloud_project=`gcloud config get-value project 2>/dev/null || echo`
    default_project=${GOOGLE_CLOUD_PROJECT:-$gcloud_project}
    if [[ -z ${default_project} ]]; then
      echo -n "Enter BigQuery project_id: "
    else
      echo -n "Enter BigQuery project_id ($default_project): "
    fi
    read -r project
    if [[ -z ${project} && -z {default_project} ]]; then
      echo -e "${RED}No BigQuery project specified, exiting...${NC}"
      exit
    fi
    project=${project:-$default_project}


    echo -n "Enter BigQuery dataset (arp): "
    read -r bq_dataset
    bq_dataset=${bq_dataset:-arp}

    ask_for_incremental_saving
    start_date=${start_date:-:YYYYMMDD-90}
    end_date=${end_date:-:YYYYMMDD-1}

    if [[ $modules =~ "assets" ]]; then
      ask_for_cohorts
      ask_for_video_orientation
    fi
    if [[ $modules =~ "ios_skan" ]]; then
      ask_for_skan_queries
    fi
  else
    if [[ -z ${project} ]]; then
      echo -n "Enter BigQuery project_id: "
      read -r project
      if [[ -z ${project} ]]; then
        echo -e "${RED}No BigQuery project specified, exiting...${NC}"
        exit
      fi
    fi
    if [[ $modules =~ "ios_skan" ]]; then
      ask_for_skan_queries
    fi
  fi
  generate_bq_macros

  if [[ -n $RUNNING_IN_GCE && $generate_config_only ]]; then
    # if you're running inside Google Cloud Compute Engine as generating config
    # (see gcp/cloud-run-button/main.sh) then there's no need for additional questions
    config_file="$SCRIPT_PATH/$solution_name_lowercase.yaml"
    save_config="--save-config --config-destination=$config_file"
    echo -e "${COLOR}Saving configuration to $config_file${NC}"
    if [[ $initial_load = "y" ]]; then
      fetch_reports $save_config --log=$loglevel --api-version=$API_VERSION --dry-run --macro.initial_load_date=$initial_load_date
    else
      fetch_reports $save_config --log=$loglevel --api-version=$API_VERSION --dry-run
    fi
    generate_output_tables $save_config --log=$loglevel --dry-run
    fetch_video_orientation $save_config --log=$loglevel --dry-run
    if [[ $skan_answer = "y" ]]; then
      create_skan_schema $save_config --log=$loglevel --dry-run
    fi
    for arp_answer in backfill incremental legacy; do
      save_to_config $config_file $arp_answer
    done
    exit
  fi

  if [[ $defaults != "y" ]]; then
    echo -n "Do you want to save this config (Y/n): "
    read -r save_config_answer
    save_config_answer=$(convert_answer $save_config_answer 'Y')
    if [[ $save_config_answer = "y" ]]; then
      echo -n "Save config as ($solution_name_lowercase.yaml): "
      read -r config_file_name
      config_file_name=${config_file_name:-$solution_name_lowercase.yaml}
      config_file=$(echo "`echo $config_file_name | sed 's/\.yaml//'`.yaml")
      config_file=$SCRIPT_PATH/$config_file
    elif [[ $save_config_answer = "q" ]]; then
      exit
    else
      config_file="/tmp/app_reporting_pack.yaml"
    fi
  else
    config_file=$SCRIPT_PATH/$solution_name_lowercase.yaml
  fi
  save_config="--save-config --config-destination=$config_file"
  echo -e "${COLOR}Saving configuration to $config_file${NC}"
  if [[ $initial_load = "y" ]]; then
    fetch_reports $save_config --log=$loglevel --api-version=$API_VERSION --dry-run --macro.initial_load_date=$initial_load_date
  else
    fetch_reports $save_config --log=$loglevel --api-version=$API_VERSION --dry-run
  fi
  generate_output_tables $save_config --log=$loglevel --dry-run
  fetch_video_orientation $save_config --log=$loglevel --dry-run
  if [[ $skan_answer = "y" ]]; then
    create_skan_schema $save_config --log=$loglevel --dry-run
  fi

  for arp_answer in backfill incremental legacy; do
    save_to_config $config_file $arp_answer
  done
  if [[ $generate_config_only = "y" ]]; then
    exit
  fi
  if [[ $defaults != "y" ]]; then
    print_configuration
  fi
}

print_configuration() {
  echo "Your configuration:"
  echo "  account_id: $customer_id"
  echo "  BigQuery project_id: $project"
  echo "  BigQuery dataset: $bq_dataset"
  echo "  Reporting window: Last $start_date_days days"
  echo "  Ads config: $ads_config"
  echo "  Cohorts: $cohorts_final"
  echo "  Parse video dimensions: $video_parsing_mode_output"
  if [[ $skan_answer = "y" ]]; then
    echo "  SKAN schema mode: $skan_schema_mode"
  fi
}


run_google_ads_queries() {
  echo -e "${COLOR}===fetching ads data for $1===${NC}"
    local config_file=${2:-$config_file}
    gaarf $(dirname $0)/$1/google_ads_queries/*.sql -c=$config_file \
      --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
}

run_bq_queries() {
  if [ -d "$(dirname $0)/$1/bq_queries/snapshots/" ]; then
    echo -e "${COLOR}===generating snapshots for $1===${NC}"
    gaarf-bq $(dirname $0)/$1/bq_queries/snapshots/*.sql -c=$config_file --log=$loglevel
  fi
  if [ -d "$(dirname $0)/$1/bq_queries/views/" ]; then
    echo -e "${COLOR}===generating views for $1===${NC}"
    gaarf-bq $(dirname $0)/$1/bq_queries/views/*.sql -c=$config_file --log=$loglevel
  fi
  echo -e "${COLOR}===generating output tables for $1===${NC}"
  gaarf-bq $(dirname $0)/$1/bq_queries/*.sql -c=$config_file --log=$loglevel
  if [ -d "$(dirname $0)/$1/bq_queries/legacy_views/" ]; then
    if [[ $legacy = "y" ]]; then
      echo -e "${COLOR}===generating legacy views for $1===${NC}"
      gaarf-bq $(dirname $0)/$1/bq_queries/legacy_views/*.sql -c=$config_file --log=$loglevel
    fi
  fi

  if [ -d "$(dirname $0)/$1/bq_queries/incremental/" ]; then
    if [[ $initial_load = "y" ]]; then
      echo -e "${COLOR}===performing initial load of performance data for $1===${NC}"
      gaarf-bq $(dirname $0)/$1/bq_queries/incremental/initial_load.sql \
        --project=`echo $project` --macro.target_dataset=`echo $target_dataset` \
        --macro.initial_date=`echo $initial_date` \
        --macro.start_date=`echo $start_date` --log=$loglevel
    else
      infer_answer_from_config $config_file incremental
      if [[ $incremental = "y" ]]; then
        echo -e "${COLOR}===saving incremental performance data for $1===${NC}"
        gaarf-bq $(dirname $0)/$1/bq_queries/incremental/incremental_saving.sql \
        --project=`echo $project` --macro.target_dataset=`echo $target_dataset` \
        --macro.initial_date=`echo $initial_date` \
        --macro.start_date=`echo $start_date` --log=$loglevel
      fi
    fi
  fi
}

define_runtime_config () {
  local incremental_table=${1:-ad_group_network_split}
  new_start_date=`$(which python3) $(dirname $0)/scripts/backfill_snapshots.py \
    -c=$config_file \
    --restore-incremental-snapshots \
    --incremental-table $incremental_table \
    --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION`
  if [[ ! -z $new_start_date ]]; then
    echo "table '$incremental_table' has missing performance snapshots."
    echo "adjusting start_date to '$new_start_date'."
    cat $config_file | sed "/start_date/s/${start_date}/'${new_start_date}'/g" > \
      /tmp/$solution_name_lowercase.yaml
    runtime_config=/tmp/$solution_name_lowercase.yaml
  else
    echo "table '$incremental_table' doesn't have missing performance snapshots."
    runtime_config=$config_file
  fi
  if [[ $initial_load = "y" ]];
  then
    cat $runtime_config | sed '/start_date/d;' | \
            sed 's/initial_load_date/start_date/' > /tmp/$solution_name_lowercase.yaml
    runtime_config=/tmp/$solution_name_lowercase.yaml
  fi
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
  if [[ $modules =~ "core" ]]; then
    echo -e "${COLOR}===Running 'core' module===${NC}"
    check_initial_load "ad_group_network_split"
    check_missing_incremental_snapshot "ad_group_network_split"
    define_runtime_config "ad_group_network_split"
    run_google_ads_queries "core" $runtime_config
    echo -e "${COLOR}===calculating conversion lag adjustment===${NC}"
    $(which python3) $(dirname $0)/scripts/conv_lag_adjustment.py \
      -c=$config_file \
      --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
    infer_answer_from_config $config_file backfill
    if [[ $backfill = "y" ]]; then
      echo -e "${COLOR}===backfilling bid budget snapshots===${NC}"
        $(which python3) $(dirname $0)/scripts/backfill_snapshots.py \
          -c=$config_file \
          --restore-bid-budgets \
          --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
    fi
    run_bq_queries "core"
  fi
  if [[ $modules =~ "assets" ]]; then
    echo -e "${COLOR}===Running 'assets' module===${NC}"
    check_initial_load "asset_performance"
    check_missing_incremental_snapshot "asset_performance"
    define_runtime_config "asset_performance"
    run_google_ads_queries "assets" $runtime_config
    echo -e "${COLOR}===getting video orientation===${NC}"
    $(which python3) $(dirname $0)/scripts/fetch_video_orientation.py \
      -c=$config_file \
      --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
    infer_answer_from_config $config_file backfill
    if [[ $backfill = "y" ]]; then
      echo -e "${COLOR}===backfilling asset cohort snapshots===${NC}"
        $(which python3) $(dirname $0)/scripts/backfill_snapshots.py \
          -c=$config_file \
          --restore-cohorts \
          --ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
    fi
    run_bq_queries "assets"
  fi
  if [[ $modules =~ "disapprovals" ]]; then
    echo -e "${COLOR}===Running 'disapprovals' module===${NC}"
    run_google_ads_queries "disapprovals"
    run_bq_queries "disapprovals"
  fi
  if [[ $modules =~ "geo" ]]; then
    echo -e "${COLOR}===Running 'geo' module===${NC}"
    check_initial_load "geo_performance"
    check_missing_incremental_snapshot "geo_performance"
    define_runtime_config "geo_performance"
    run_google_ads_queries "geo"
    run_bq_queries "geo"
  fi
  if [[ $modules =~ "ios_skan" ]]; then
    echo -e "${COLOR}===Running 'ios_skan' module===${NC}"
    check_initial_load "skan_decoder"
    check_missing_incremental_snapshot "skan_decoder"
    define_runtime_config "skan_decoder"
    if cat "$config_file" | grep -q skan_mode:; then
    run_google_ads_queries "ios_skan" $runtime_config
    echo -e "${COLOR}===getting SKAN schema===${NC}"
    $(which python3) $(dirname $0)/scripts/create_skan_schema.py -c=$config_file
    run_bq_queries "ios_skan"
    fi
  fi
}

check_gaarf_version
check_ads_config $validate_ads_config

# defaults
start_date_days=90
start_date=":YYYYMMDD-90"
end_date=":YYYYMMDD-1"
bq_dataset="arp"
gcloud_project=`gcloud config get-value project 2>/dev/null || echo`
project=${GOOGLE_CLOUD_PROJECT:-$gcloud_project}
parse_yaml $ads_config "GOOGLE_ADS_"
customer_id=$GOOGLE_ADS_login_customer_id
video_parsing_mode_output="placeholders"
cohorts_final="1,3,5,7,14,30"
skan_schema_mode="placeholders"
generate_bq_macros

if [[ -z ${loglevel} ]]; then
  loglevel="INFO"
fi

if [[ $generate_config_only = "y" ]]; then
  welcome
  setup
fi

if [[ -n "$config_file" || -f $solution_name_lowercase.yaml ]]; then
  config_file=${config_file:-$solution_name_lowercase.yaml}
  if [[ $quiet = "y" ]]; then
    run_with_config
  else
    echo -e "${COLOR}Found saved configuration at $config_file${NC}"
    echo -e "${COLOR}If you want to provide alternative configuration use '-c path/to/config.yaml' and restart.${NC}"
    if [[ -f "$config_file" ]]; then
      cat $config_file
    fi
    echo -n -e "${COLOR}Do you want to use this configuration? (Y/n) or press Q to quit: ${NC}"
    read -r setup_config_answer
    setup_config_answer=$(convert_answer $setup_config_answer 'Y')
    if [[ $setup_config_answer = "y" ]]; then
      echo -e "${COLOR}Using saved configuration...${NC}"
      run_with_config $config_file
    elif [[ $setup_config_answer = "n" ]]; then
      echo -e "${COLOR}Setting up new configuration... (Press Ctrl + C to exit)${NC}"
      welcome
      setup
      prompt_running
      run_with_config
    else
      echo "Exiting"
      exit
    fi
  fi
else
  welcome
  setup
  prompt_running
  run_with_config
fi
