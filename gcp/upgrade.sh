#!/bin/bash
#
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CYAN='\033[0;36m' # Cyan
WHITE='\033[0;37m'  # White

SCRIPT_PATH=$(readlink -f "$0" | xargs dirname)
SETTING_FILE="${SCRIPT_PATH}/settings.ini"
NAME=$(git config -f $SETTING_FILE config.name)
PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
APP_CONFIG_FILE=$(eval echo $(git config -f $SETTING_FILE config.config-file))
CGS_APP_CONFIG_FILE=gs://$PROJECT_ID/$NAME/$APP_CONFIG_FILE
OBSOLETE_CONFIG=0

_check_api_version() {
    local api_version=$(cat /tmp/arp.yaml | grep api_version |\
        cut -d ":" -f2 | grep -oE '[0-9]+([.][0-9]+)?')
    if (( $api_version < 17 )); then
        echo "Unsupported API version."
        echo "Recommended to update to Google Ads API 18."
        OBSOLETE_CONFIG=$(($OBSOLETE_CONFIG+1))
    fi
}

_check_scripts_section() {
    _check_section_presense "skan_mode"
    OBSOLETE_CONFIG=$(($OBSOLETE_CONFIG+$?))
}

_check_gaarf_section() {
    _check_api_version
    OBSOLETE_CONFIG=$(($OBSOLETE_CONFIG+$?))
}

_check_gaarf_bq_section() {
    _check_section_presense "has_skan"
    OBSOLETE_CONFIG=$(($OBSOLETE_CONFIG+$?))
}

_check_runtime_section() {
    _check_section_presense "incremental"
    OBSOLETE_CONFIG=$(($OBSOLETE_CONFIG+$?))
    _check_section_presense "backfill"
    OBSOLETE_CONFIG=$(($OBSOLETE_CONFIG+$?))
}

_check_section_presense() {
    local flag=$(cat /tmp/arp.yaml | grep $1 | wc -l)
    local message=${2:-Missing $1 settings flag.}
    if (( $flag  == 0 )); then
        echo $message
        return 1
    fi
    return 0
}


check_installation() {
    gsutil -q stat $CGS_APP_CONFIG_FILE
    config_exists=$?
    if [[ $config_exists -eq 0 ]]; then
        gsutil cat $CGS_APP_CONFIG_FILE | tee /tmp/arp.yaml
    else
        echo "App reporting pack isn't installed in project $PROJECT_ID"
        echo "Please install it with './gcp/install.sh' command."
        exit
    fi
    _check_gaarf_section
    _check_gaarf_bq_section
    _check_scripts_section
    _check_runtime_section
}

check_installation

pushd $SCRIPT_PATH >/dev/null
cd ..

if (( $OBSOLETE_CONFIG > 0 )); then
    echo -e "${CYAN}Obsolete configuration detected.${WHITE}"
    # Install ARP dependencies
    echo -e "${CYAN}Creating Python virtual environment...${WHITE}"
    if [[ ! -d .venv ]];  then
        python3 -m venv .venv
    fi
    . .venv/bin/activate
    pip install --require-hashes -r ./app/requirements.txt --no-deps

    # generate ARP configuration
    echo -e "${CYAN}Generating configuration...${WHITE}"
    RUNNING_IN_GCE=true
    ./app/run-local.sh --generate-config-only
fi

# deploy solution
echo -e "${CYAN}Upgrading application...${WHITE}"
./gcp/setup.sh copy_application_scripts build_docker_image_gcr start

popd >/dev/null

