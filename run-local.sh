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

#!/bin/bash
COLOR='\033[0;36m' # Cyan
NC='\033[0m' # No color
usage="bash run-local.sh -c|--config <config> -q|--quiet\n\n
Helper script for running App Reporting Pack queries.\n\n
-h|--help - show this help message\n
-c|--config <config> - path to config.yaml file, i.e., path/to/app_reporting_pack.yaml\n
-q|--quiet - skips all confirmation prompts and starts running scripts based on config files
--legacy - generates legacy views that can be plugin into existing legacy dashboard
"

solution_name="App Reporting Pack"
solution_name_lowercase=$(echo $solution_name | tr '[:upper:]' '[:lower:]' |\
	tr ' ' '_')

config_file="$solution_name_lowercase.yaml"
quiet="n"

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
		shift
		legacy="y"
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

check_ads_config() {
	if [[ -n $google_ads_config ]]; then
		ads_config=$google_ads_config
	elif [[ -f "$HOME/google-ads.yaml" ]]; then
		ads_config=$HOME/google-ads.yaml
	else
		echo -n "Enter full path to google-ads.yaml file: "
		read -r ads_config
	fi
}

convert_answer() {
	echo "$1" | tr '[:upper:]' '[:lower:]' | cut -c1
}

setup() {
	echo -n "Enter account_id: "
	read -r customer_id
	echo -n "Enter BigQuery project_id: "
	read -r project
	echo -n "Enter BigQuery dataset: "
	read -r bq_dataset
	echo -n "Enter start_date in YYYY-MM-DD format (or use :YYYYMMDD-30 for last 30 days): "
	read -r start_date
	echo -n "Enter end_date in YYYY-MM-DD format (or use :YYYYMMDD-1 for yesterday): "
	read -r end_date
	ask_for_cohorts
	echo  "Script are expecting google-ads.yaml file in your home directory"
	echo -n "Is the file there (Y/n): "
	read -r ads_config_answer
	ads_config_answer=$(convert_answer $ads_config_answer)
	if [[ $ads_config_answer = "y" ]]; then
		ads_config=$HOME/google-ads.yaml
	else
		echo -n "Enter full path to google-ads.yaml file: "
		read -r ads_config
	fi
	echo -n "Do you want to save this config (Y/n): "
	read -r save_config_answer
	save_config_answer=$(convert_answer $save_config_answer)
	if [[ $save_config_answer = "y" ]]; then
		save_config="--save-config --config-destination=$solution_name_lowercase.yaml"
	elif [[ $save_config_answer = "q" ]]; then
		exit 1
	fi
	print_configuration
}


deploy() {
	echo -n -e "${COLOR}Deploy $solution_name? Y/n/q: ${NC}"
	read -r answer
	answer=$(convert_answer $answer)

	if [[ $answer = "y" ]]; then
		echo "Deploying..."
	elif [[ $answer = "q" ]]; then
		exit 1
	else
		setup
	fi
	generate_parameters
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

generate_parameters() {
	bq_dataset_output=$(echo $bq_dataset"_output")
	bq_dataset_legacy=$(echo $bq_dataset"_legacy")
	macros="--macro.bq_dataset=$bq_dataset --macro.target_dataset=$bq_dataset_output --macro.legacy_dataset=$bq_dataset_legacy --template.cohort_days=$cohorts_final"
}


fetch_reports() {
	echo -e "${COLOR}===fetching reports===${NC}"
	gaarf google_ads_queries/*/*.sql \
	--account=$customer_id \
	--output=bq \
	--customer-ids-query="$customer_ids_query" \
	--bq.project=$project --bq.dataset=$bq_dataset \
	--macro.start_date=$start_date --macro.end_date=$end_date \
	--ads-config=$ads_config "$@"
}

conversion_lag_adjustment() {
	echo -e "${COLOR}===calculating conversion lag adjustment===${NC}"
	$(which python3) scripts/conv_lag_adjustment.py \
		--account=$customer_id --ads-config=$ads_config \
		--bq.project=$project --bq.dataset=$bq_dataset
}

generate_bq_views() {
	echo -e "${COLOR}===generating views and functions===${NC}"
	gaarf-bq bq_queries/views_and_functions/*.sql \
		--project=$project --target=$bq_dataset $macros "$@"
}


generate_snapshots() {
	echo -e "${COLOR}===generating snapshots===${NC}"
	gaarf-bq bq_queries/snapshots/*.sql \
		--project=$project --target=$bq_dataset $macros "$@"
}

generate_output_tables() {
	echo -e "${COLOR}===generating final tables===${NC}"
	gaarf-bq bq_queries/*.sql \
		--project=$project --target=$bq_dataset_output $macros "$@"
}

generate_legacy_views() {
	echo -e "${COLOR}===generating legacy views===${NC}"
	gaarf-bq bq_queries/legacy_views/*.sql \
		--project=$project --target=$bq_dataset_legacy $macros "$@"
}

print_configuration() {
	echo "Your configuration:"
	echo "	account_id: $customer_id"
	echo "	BigQuery project_id: $project"
	echo "	BigQuery dataset:: $bq_dataset"
	echo "	Start date: $start_date"
	echo "	End date: $end_date"
	echo "	Ads config: $ads_config"
	echo "	Cohorts: $cohorts_final"
}

welcome() {
	echo -e "${COLOR}Welcome to installation of $solution_name${NC} "
}

get_input() {
	setup
	deploy
}


run_with_config() {
	echo -e "${COLOR}===fetching reports===${NC}"
	gaarf google_ads_queries/**/*.sql -c=$solution_name_lowercase.yaml \
		--ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
	echo -e "${COLOR}===calculating conversion lag adjustment===${NC}"
	$(which python3) scripts/conv_lag_adjustment.py \
		-c=$solution_name_lowercase.yaml \
		--ads-config=$ads_config --log=$loglevel --api-version=$API_VERSION
	echo -e "${COLOR}===generating snapshots===${NC}"
	gaarf-bq bq_queries/snapshots/*.sql -c=$solution_name_lowercase.yaml --log=$loglevel
	echo -e "${COLOR}===generating views and functions===${NC}"
	gaarf-bq bq_queries/views_and_functions/*.sql -c=$solution_name_lowercase.yaml --log=$loglevel
	echo -e "${COLOR}===generating final tables===${NC}"
	gaarf-bq bq_queries/*.sql -c=$solution_name_lowercase.yaml --log=$loglevel
	if [[ $legacy = "y" ]]; then
		echo -e "${COLOR}===generating legacy views===${NC}"
		gaarf-bq bq_queries/legacy_views/*.sql -c=$solution_name_lowercase.yaml --log=$loglevel
	fi

}

check_ads_config

if [[ -z ${loglevel} ]]; then
	loglevel="INFO"
fi

if [[ -f "$config_file" ]]; then
	if [[ $quiet = "n" ]]; then
		echo -e "${COLOR}Found saved configuration at $config_file${NC}"
		cat $config_file
		echo -n -e "${COLOR}Do you want to use it (Y/n/q): ${NC}"
		read -r setup_config_answer
		setup_config_answer=$(convert_answer $setup_config_answer)
		if [[ $setup_config_answer = "y" ]]; then
			echo -e "${COLOR}Using saved configuration...${NC}"
			run_with_config
		elif [[ $setup_config_answer = "q" ]]; then
			exit 1
		else
			echo
			welcome
			get_input
		fi
	else
		run_with_config
	fi
else
	welcome
	get_input
	fetch_reports $save_config --log=$loglevel --api-version=$API_VERSION
	conversion_lag_adjustment --customer--ids-query="$customer_ids_query" \
		--api-version=$API_VERSION
	generate_snapshots $save_config --log=$loglevel
	generate_bq_views $save_config --log=$loglevel
	generate_output_tables $save_config --log=$loglevel
	if [[ $legacy = "y" ]]; then
		generate_legacy_views $save_config --log=$loglevel
	fi
fi
