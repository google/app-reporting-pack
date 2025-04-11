#!/bin/bash
#
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCRIPT_PATH=$(readlink -f "$0" | xargs dirname)

api_version=$1
echo "api_version: $api_version"
errors=$(gaarf-simulator $SCRIPT_PATH/../app/*/google_ads_queries/*.sql \
  --account 1 \
  --macro.start_date=:YYYYMMDD-1 \
  --macro.end_date=:YYYYMMDD-1 \
  --api-version=$api_version > /dev/null)
if [[ ! -z $errors ]]; then
  echo "Found errors in the Google Ads API queries."
  exit 1
fi
