#!/bin/bash

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


COLOR='\033[0;36m'
service_account_name="app-reporting-pack-ext-service"
service_account_email="$service_account_name@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com"

echo -n -e "${COLOR}Creating Service Account..."

gcloud iam service-accounts create $service_account_name --display-name $service_account_name 

gcloud run services add-iam-policy-binding app-reporting-pack \
   --member=serviceAccount:$service_account_email \
   --role=roles/run.invoker \
   --region=${GOOGLE_CLOUD_REGION}

gcloud projects add-iam-policy-binding ${GOOGLE_CLOUD_PROJECT} \
     --member=serviceAccount:$service_account_email\
     --role=roles/iam.serviceAccountTokenCreator

echo -n -e "${COLOR}Enabling Google Ads API..."
gcloud services enable googleads.googleapis.com

echo -n -e "${COLOR}Creating Scheduler..."
gcloud scheduler jobs create http daily-data-refresh \
    --location=${GOOGLE_CLOUD_REGION} \
    --schedule="0 4 * * *" \
    --uri=${SERVICE_URL}/run-queries \
    --description="Triggering queries run"

echo -n -e "${COLOR}Extending service timeout limit..."
gcloud run services update app-reporting-pack --timeout=3600 --region=${GOOGLE_CLOUD_REGION}

echo -n -e "${COLOR}Triggering first queries run..."
gcloud scheduler jobs run daily-data-refresh
