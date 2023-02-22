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

set -e
echo "run-docker script triggered"

# TODO: this file is unsync with run-local.sh (not clear why it's needed at all)
ads_queries=$1
bq_queries=$2
ads_yaml=$3
config_yaml=$4
gaarf $ads_queries -c=$config_yaml --ads-config=$ads_yaml
python3 scripts/conv_lag_adjustment.py -c=$config_yaml --ads-config=$ads_yaml
gaarf-bq $bq_queries/snapshots/*.sql -c=$config_yaml
gaarf-bq $bq_queries/views_and_functions/*.sql -c=$config_yaml
gaarf-bq $bq_queries/*.sql -c=$config_yaml
gaarf-bq $bq_queries/legacy_views/*.sql  -c=$config_yaml
