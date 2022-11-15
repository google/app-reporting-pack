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
quiet="N"

while :; do
case $1 in
	-q|--quiet)
		quiet="Y"
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
		legacy="Y"
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

setup() {
	echo -n "Enter account_id: "
	read -r customer_id
	echo -n "Enter BigQuery project_id: "
	read -r project
	echo -n "Enter BigQuery dataset: "
	read -r bq_dataset
	echo -n "Enter start_date in YYYY-MM-DD format: "
	read -r start_date
	echo -n "Enter end_date in YYYY-MM-DD format: "
	read -r end_date
	ask_for_cohorts
	echo  "Script are expecting google-ads.yaml file in your home directory"
	echo -n "Is the file there (Y/n): "
	read -r ads_config_answer
	if [[ $ads_config_answer = "Y" ]]; then
		ads_config=$HOME/google-ads.yaml
	else
		echo -n "Enter full path to google-ads.yaml file: "
		read -r ads_config
	fi
	echo -n "Do you want to save this config (Y/n): "
	read -r save_config_answer
	if [[ $save_config_answer = "Y" ]]; then
		save_config="--save-config --config-destination=$solution_name_lowercase.yaml"
	fi
	print_configuration
}


deploy() {
	echo -n -e "${COLOR}Deploy $solution_name? Y/n/q: ${NC}"
	read -r answer

	if [[ $answer = "Y" ]]; then
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
	if [[ $cohorts_answer = "Y" ]]; then
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
	$(which python) scripts/conv_lag_adjustment.py \
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
		--ads-config=$ads_config --log=$loglevel
	echo -e "${COLOR}===calculating conversion lag adjustment===${NC}"
	$(which python) scripts/conv_lag_adjustment.py \
		-c=$solution_name_lowercase.yaml \
		--ads-config=$ads_config --log=$loglevel
	echo -e "${COLOR}===generating snapshots===${NC}"
	gaarf-bq bq_queries/snapshots/*.sql -c=$solution_name_lowercase.yaml --log=$loglevel
	echo -e "${COLOR}===generating views and functions===${NC}"
	gaarf-bq bq_queries/views_and_functions/*.sql -c=$solution_name_lowercase.yaml --log=$loglevel
	echo -e "${COLOR}===generating final tables===${NC}"
	gaarf-bq bq_queries/*.sql -c=$solution_name_lowercase.yaml --log=$loglevel
	if [[ $legacy = "Y" ]]; then
		echo -e "${COLOR}===generating legacy views===${NC}"
		gaarf-bq bq_queries/legacy_views/*.sql -c=$solution_name_lowercase.yaml --log=$loglevel
	fi

}

check_ads_config

if [[ -z ${loglevel} ]]; then
	loglevel="INFO"
fi

if [[ -f "$config_file" ]]; then
	if [[ $quiet = "N" ]]; then
		echo -e "${COLOR}Found saved configuration at $solution_name_lowercase.yaml${NC}"
		cat $solution_name_lowercase.yaml
		echo -n -e "${COLOR}Do you want to use it (Y/n): ${NC}"
		read -r setup_config_answer
		if [[ $setup_config_answer = "Y" ]]; then
			echo -e "${COLOR}Using saved configuration...${NC}"
		fi
	fi
	run_with_config
else
	welcome
	get_input
	fetch_reports $save_config --log=$loglevel
	conversion_lag_adjustment --customer--ids-query="$customer_ids_query"
	generate_snapshots $save_config --log=$loglevel
	generate_bq_views $save_config --log=$loglevel
	generate_output_tables $save_config --log=$loglevel
	if [[ $legacy = "Y" ]]; then
		generate_legacy_views $save_config --log=$loglevel
	fi
fi
