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

ask_for_video_orientation() {
  echo -n "Video orientations can be fetched from YouTube [y] or inferred from asset names [r], which mode do you prefer: [y/r]: "
  read -r video_parsing_mode
  video_parsing_mode=$(convert_answer $video_parsing_mode)
  if [[ $video_parsing_mode = "r" ]]; then
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
      let "orientation_position_view=orientation_position+1"
      video_orientation=`echo ${splitted[$orientation_position_view]}`
      echo "Video orientation: $video_orientation"
    fi
    echo -n "Select the orientation_delimiter: "
    read -r orientation_delimiter
    orientation_delimiter=$(convert_answer $orientation_delimiter)
    if [[ $template_string != "q" ]]; then
      echo "Video width will be `echo $video_orientation | cut -d $orientation_delimiter -f1`"
      echo "Video height will be `echo $video_orientation | cut -d $orientation_delimiter -f2`"
    fi
  elif [[ $video_parsing_mode = "y" ]]; then
    video_parsing_mode_output="youtube"
    echo "Need something"

  else
    video_parsing_mode_output="placeholders"
  fi
}

ask_for_cohorts() {
  default_cohorts=(0 1 3 5 7 14 30)
  echo -n -e "${COLOR}Asset performance has cohorts for 0,1,3,5,7,14 and 30 days. Do you want to adjust it? [Y/n]: ${NC}"
  read -r cohorts_answer
  ads_config_answer=$(convert_answer $cohorts_answer)
  if [[ $cohorts_answer = "y" ]]; then
    echo -n -e "${COLOR}Please enter cohort number in the following format 1,2,3,4,5: ${NC}"
    read -r cohorts_string
  fi
  IFS="," read -ra cohorts_array <<< $cohorts_string
  combined_cohorts=("${default_cohorts[@]}" "${cohorts_array[@]}")
  unique_cohorts=($(for f in "${combined_cohorts[@]}"; do echo "${f}"; done | sort -u -n))
  _cohorts="${unique_cohorts[@]}"
  cohorts_final=$(echo ${_cohorts// /,})
}

generate_bq_macros() {
  bq_dataset_output=$(echo $bq_dataset"_output")
  bq_dataset_legacy=$(echo $bq_dataset"_legacy")
  macros="--macro.bq_dataset=$bq_dataset --macro.target_dataset=$bq_dataset_output --macro.legacy_dataset=$bq_dataset_legacy --template.cohort_days=$cohorts_final"
}
