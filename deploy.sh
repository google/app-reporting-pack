#!/bin/bash
welcome() {
echo "Welcome to installation of App Reporting Pack"
	if [[ -f "reporting_pack.config" ]]; then
		read_config
		echo "Found saved configuration."
		print_configuration
		echo -n "Do you want to use it (Y/n): "
		read -r setup_config_answer
		if [[ $setup_config_answer = "Y" ]]; then
			echo "Using saved configuration..."
		else
			echo "Creating new configuration..."
			setup
		fi
	else
		setup
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
		save_config
	fi
	print_configuration
}

save_config() {
	declare -A setup_config
	setup_config["customer_id"]=$customer_id
	setup_config["project"]=$project
	setup_config["bq_dataset"]=$bq_dataset
	setup_config["start_date"]=$start_date
	setup_config["end_date"]=$end_date
	setup_config["ads_config"]=$ads_config
	setup_config["cohorts"]=$cohorts
	declare -p setup_config > "reporting_pack.config"
}

read_config() {
	declare -A config
	source -- "reporting_pack.config"
	customer_id=${setup_config["customer_id"]}
	project=${setup_config["project"]}
	bq_dataset=${setup_config["bq_dataset"]}
	start_date=${setup_config["start_date"]}
	end_date=${setup_config["end_date"]}
	ads_config=${setup_config["ads_config"]}
	cohorts=${setup_config["cohorts"]}
}

deploy() {
	echo -n "Deploy App Reporting Pack? Y/n/q: "
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
	echo -n "Asset performance can have cohorts. Enter Y if you want to activate: "
	read -r cohorts_answer
	if [[ $cohorts_answer = "Y" ]]; then
		echo -n "Please enter cohort number in the following format 1,2,3,4,5: "
		read -r cohorts
	fi
}
generate_parameters() {
	bq_dataset_output=$(echo $bq_dataset"_output")
	bq_dataset_legacy=$(echo $bq_dataset"_legacy") macros="--macro.bq_project=$project --macro.bq_dataset=$bq_dataset --macro.target_dataset=$bq_dataset_output --macro.legacy_dataset=$bq_dataset_legacy --template.cohort_days=$cohorts"
}


fetch_reports() {
	echo "===fetching reports==="
	gaarf google_ads_queries/*/*.sql \
	--account=$customer_id \
	--output=bq \
	--bq.project=$project --bq.dataset=$bq_dataset \
	--macro.start_date=$start_date --macro.end_date=$end_date \
	--ads-config=$ads_config
}

conversion_lag_adjustment() {

	echo "===calculating conversion lag adjustment==="
	$(which python) scripts/conv_lag_adjustment.py \
		--bq.project=$project --bq.dataset=$bq_dataset
}

generate_bq_views() {
	echo "===generating views and functions==="
	gaarf-bq bq_queries/views_and_functions/*.sql \
		--project=$project --target=$bq_dataset $macros
}


generate_snapshots() {
	echo "===generating snapshots==="
	gaarf-bq bq_queries/snapshots/*.sql \
		--project=$project --target=$bq_dataset $macros
}

generate_output_tables() {
	echo "===generating final tables==="
	gaarf-bq bq_queries/*.sql \
		--project=$project --target=$bq_dataset $macros
}

generate_legacy_views() {
	echo "===generating legacy views==="
	gaarf-bq bq_queries/legacy_views/*.sql \
		--project=$project --target=$bq_dataset $macros
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

welcome
deploy
fetch_reports
conversion_lag_adjustment
generate_bq_views
generate_snapshots
generate_output_tables
generate_legacy_views
