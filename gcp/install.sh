#!/bin/bash
#
# Copyright 2022 Google LLC
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

SCRIPT_PATH=$(readlink -f "$0" | xargs dirname)
# changing the cwd to the script's contining folder so all pathes inside can be local to it
# (important as the script can be called via absolute path and as a nested path)
pushd $SCRIPT_PATH >/dev/null
cd ..

CYAN='\033[0;36m' # Cyan
WHITE='\033[0;37m'  # White

# Install ARP dependencies
echo -e "${CYAN}Creating Python virtual environment...${WHITE}"
python3 -m venv .venv
. .venv/bin/activate
pip install --require-hashes -r ./app/requirements.txt --no-deps

# generate ARP configuration
echo -e "${CYAN}Generating configuration...${WHITE}"
RUNNING_IN_GCE=true
export RUNNING_IN_GCE   # signaling to run-local.sh that we're runnign inside GCE (there'll be less questions)
./app/run-local.sh --generate-config-only --validate-google-ads-config

# deploy solution
echo -e "${CYAN}Deploying Cloud components...${WHITE}"
#./gcp/setup.sh deploy_public_index deploy_all start
./gcp/setup.sh deploy_all start

popd >/dev/null
