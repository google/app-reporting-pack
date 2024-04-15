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
COLOR='\033[0;36m' # Cyan
NC='\033[0m' # No color

ask_for_skan_queries() {
  skan_answer="y"
  echo -e "${COLOR}App Reporting Pack SKAN Reports can be enriched with decoded SKAN conversion values${NC}"
  echo -e "${COLOR}(More at https://github.com/google/app-reporting-pack/blob/main/docs/how-to-specify-ios-skan-schema.md):${NC}"
  echo -n "Enter fully qualified table name in BigQuery (PROJECT.DATASET.TABLE_NAME) or press Enter to skip: "
  read -r skan_schema_input_table
  if [[ -z $skan_schema_input_table  ]]; then
    skan_schema_mode="placeholders"
  else
    skan_schema_mode="table"
  fi
}

ask_for_video_orientation() {
  echo -e "${COLOR}Setup asset video orientation parsing:${NC}"
  echo -e "${COLOR}(More at https://github.com/google/app-reporting-pack/blob/main/docs/how-to-get-video-orientation-for-assets.md):${NC}"
  echo -e "Please select one of the following options:"
  echo -e "\t1. Video dimension is encoded in asset names."
  echo -e "\t2. I can access YouTube Data API (needs authorization) to fetch orientation from there."
  echo -e "\t3. Skip for now"
  echo -n "Specify option or Press Enter to skip:  "
  read -r video_parsing_mode
  video_parsing_mode=$(convert_answer $video_parsing_mode)
  video_parsing_mode=${video_parsing_mode:-3}
  if [[ $video_parsing_mode = "1" ]]; then
    video_parsing_mode_output="regex"
    echo -n "(Optional) Provide sample asset_name to validate parsing [Q to skip]: "
    read -r template_string
    echo -n "Select the delimiter for asset_name: "
    read -r element_delimiter
    if [[ $template_string != "q" ]]; then
      element_delimiter=$(convert_answer $element_delimiter)
      IFS="$element_delimiter" read -ra splitted <<< "$template_string"
      for i in "${!splitted[@]}"; do
        printf "$i) ${splitted[$i]}; "
      done
      echo
    fi

    echo -n "Select the orientation position (starting from zero): "
    read -r orientation_position
    orientation_position=$(convert_answer $orientation_position)

    if [[ $template_string != "q" ]]; then
      video_orientation=`echo ${splitted[$orientation_position]}`
      echo "Video orientation: $video_orientation"
    fi
    echo -n "Select the orientation_delimiter: "
    read -r orientation_delimiter
    orientation_delimiter=$(convert_answer $orientation_delimiter)
    if [[ $template_string != "q" ]]; then
      echo "Video width will be `echo $video_orientation | cut -d $orientation_delimiter -f1`"
      echo "Video height will be `echo $video_orientation | cut -d $orientation_delimiter -f2`"
    fi
  elif [[ $video_parsing_mode = "2" ]]; then
    video_parsing_mode_output="youtube"
    echo -n "Please enter path to youtube_config.yaml file: "
    read -r youtube_config_path
  else
    video_parsing_mode_output="placeholders"
  fi
}

ask_for_cohorts() {
  default_cohorts=(1 3 5 7 14 30)
  echo -e "${COLOR}App Reporting Pack will calculate cohort stats for conversion lags 1,3,5,7,14,30.${NC}"
  echo -n "Provide additional conversion lag days as comma separated list or press Enter to use the ones above: "
  read -r cohorts_string
  cohorts_string=${cohorts_string:-1}
  IFS="," read -ra cohorts_array <<< $cohorts_string
  combined_cohorts=("${default_cohorts[@]}" "${cohorts_array[@]}")
  # TODO: Always exclude 0 cohort
  unique_cohorts=($(for f in "${combined_cohorts[@]}"; do echo "${f}"; done | sort -u -n))
  _cohorts="${unique_cohorts[@]}"
  cohorts_final=$(echo ${_cohorts// /,})
}

ask_for_incremental_saving() {
  bq_dataset_output=$(echo $bq_dataset"_output")
  echo -e "${COLOR}App Reporting Pack will extract fresh performance data from Google Ads API within selected reporting window (i.e. last 30 days).${NC}"
  echo -e "${COLOR}(More at https://github.com/google/app-reporting-pack/blob/main/docs/storing-data-for-extended-period.md):${NC}"
  get_start_end_date
  echo -n "Download historical performance data starting from YYYY-MM-DD or press Enter to skip: "
  read -r initial_load_date
  if [ ! -z $initial_load_date ]; then
    validate_load_date $initial_load_date
    if [[ $invalid_initial_load_date = "y" ]]; then
      echo -e "\tYou've entered invalid initial date"
      echo -n -e "\tPlease provide initial date in YYYY-MM-DD format (i.e. 2023-01-01) or press Enter to skip: "
      read -r initial_load_date
      validate_load_date $initial_load_date
    fi
    if [[ $invalid_initial_load_date = "y" ]]; then
      echo "invalid initial load date ($invalid_load_date), skipping initial load"
    elif [[ "$start_date" =~ ":YYYYMMDD-" ]]; then
      initial_load="y"
    else
      echo "start_date is not dynamic ($start_date), skipping initial load"
    fi
  fi
}

generate_bq_macros() {
  bq_dataset_output=$(echo $bq_dataset"_output")
  bq_dataset_legacy=$(echo $bq_dataset"_legacy")
  skan_schema_table=$(echo $bq_dataset".skan_schema")
  macros=$(echo --macro.bq_dataset=$bq_dataset --macro.target_dataset=$bq_dataset_output --macro.legacy_dataset=$bq_dataset_legacy  --template.cohort_days=$cohorts_final --macro.skan_schema_input_table=$skan_schema_input_table)
  if [[ $skan_answer = "y" ]]; then
    macros="$macros --template.has_skan=true"
  fi
  if [[ $initial_load = "y" || $incremental = "y" ]]; then
    macros="$macros --template.incremental=true"
  fi
}

get_start_end_date() {
  echo -n -e "Enter reporting window in days (default '90 days' but not less than conversion window): "
  read -r start_date_days
  start_date=":YYYYMMDD-${start_date_days:-90}"
  end_date=":YYYYMMDD-1"
}

validate_load_date() {
  local _initial_load_date=$1
  if [[ ! $_initial_load_date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2} ]]; then
    invalid_initial_load_date="y"
  else
    invalid_initial_load_date="n"
  fi
}
