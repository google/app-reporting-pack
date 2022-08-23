#!/bin/bash
COLOR='\033[0;36m' # Cyan
NC='\033[0m' # No color

solution_name="App Reporting Pack"
solution_name_lowercase=$(echo $solution_name | tr '[:upper:]' '[:lower:]' |\
	tr ' ' '_')

check_ads_config() {
	if [[ -f "$HOME/google-ads.yaml" ]]; then
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
	echo -n -e "${COLOR}Asset performance can have cohorts. Enter Y if you want to activate: ${NC}"
	read -r cohorts_answer
	if [[ $cohorts_answer = "Y" ]]; then
		echo -n -e "${COLOR}Please enter cohort number in the following format 1,2,3,4,5: ${NC}"
		read -r cohorts
	fi
}
generate_parameters() {
	bq_dataset_output=$(echo $bq_dataset"_output")
	bq_dataset_legacy=$(echo $bq_dataset"_legacy")
	macros="--macro.bq_project=$project --macro.bq_dataset=$bq_dataset --macro.target_dataset=$bq_dataset_output --macro.legacy_dataset=$bq_dataset_legacy --template.cohort_days=$cohorts"
}


fetch_reports() {
	echo -e "${COLOR}===fetching reports===${NC}"
	gaarf google_ads_queries/*/*.sql \
	--account=$customer_id \
	--output=bq \
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
	echo "	Cohorts: $cohorts"
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
		--ads-config=$ads_config
	echo -e "${COLOR}===calculating conversion lag adjustment===${NC}"
	$(which python) scripts/conv_lag_adjustment.py \
		-c=$solution_name_lowercase.yaml \
		--ads-config=$ads_config
	echo -e "${COLOR}===generating views and functions===${NC}"
	gaarf-bq bq_queries/views_and_functions/*.sql -c=$solution_name_lowercase.yaml
	echo -e "${COLOR}===generating snapshots===${NC}"
	gaarf-bq bq_queries/snapshots/*.sql -c=$solution_name_lowercase.yaml
	echo -e "${COLOR}===generating final tables===${NC}"
	gaarf-bq bq_queries/*.sql -c=$solution_name_lowercase.yaml
	echo -e "${COLOR}===generating legacy views===${NC}"
	gaarf-bq bq_queries/legacy_views/*.sql -c=$solution_name_lowercase.yaml

}

welcome
check_ads_config

if [[ -f "$solution_name_lowercase.yaml" ]]; then
	echo -e "${COLOR}Found saved configuration at $solution_name_lowercase.yaml${NC}"
	cat $solution_name_lowercase.yaml
	echo -n -e "${COLOR}Do you want to use it (Y/n): ${NC}"
	read -r setup_config_answer
	if [[ $setup_config_answer = "Y" ]]; then
		echo -e "${COLOR}Using saved configuration...${NC}"
	fi
	run_with_config
else
	get_input
	fetch_reports $save_config
	conversion_lag_adjustment
	generate_bq_views $save_config
	generate_snapshots $save_config
	generate_output_tables $save_config
	generate_legacy_views $save_config
fi
