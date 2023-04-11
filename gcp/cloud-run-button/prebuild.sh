#!/bin/bash
set -e
#set -x

RED='\033[0;31m' # Red Color
CYAN='\033[0;36m' # Cyan
NC='\033[0m' # No Color
WHITE='\033[0;37m'  # White

echo -n -e "${WHITE}[ ? ] Would you like to use a google-ads.yaml (Y/n) - if so, please upload a file using Cloud Shell menu in the top right corner (otherewise you'll be asked to enter credentials later): ${NC}"
read -r USE_GOOGLE_ADS_CONFIG
USE_GOOGLE_ADS_CONFIG=${USE_GOOGLE_ADS_CONFIG:-y}
while [[ $USE_GOOGLE_ADS_CONFIG = "Y" || $USE_GOOGLE_ADS_CONFIG = "y" ]]; do 
  # NOTE: the script is executed inside $APP_DIR folder (not where it's located) 
  if [[ ! -f ./google-ads.yaml && -f ./../google-ads.yaml ]]; then
    cp ./../google-ads.yaml ./google-ads.yaml
  fi
  if [[ ! -f ./google-ads.yaml ]]; then    
    echo -e "${RED}Could not found google-ads.yaml config file${NC}"
    echo -n "Please upload google-ads.yaml and enter 'Y' or press Enter to skip: "
    read -r USE_GOOGLE_ADS_CONFIG
  else
    break
  fi
done
if [[ $USE_GOOGLE_ADS_CONFIG = "Y" || $USE_GOOGLE_ADS_CONFIG = "y" ]]; then
  echo "Using google-ads.yaml"
  # update Dockerfile to copy google-ads.yaml:
  sed -i -e "s|##*[[:space:]]*COPY google-ads.yaml \..*$|COPY google-ads.yaml \.|" ./Dockerfile
fi

# Inside this hook script we have available the following variables:
# $GOOGLE_CLOUD_PROJECT - Google Cloud project
# $GOOGLE_CLOUD_REGION - selected Google Cloud Region
# $K_SERVICE - Cloud Run service name
# $IMAGE_URL - container image URL
# $APP_DIR - application directory

gcloud config set project $GOOGLE_CLOUD_PROJECT

PROJECT_NUMBER=$(gcloud projects describe $GOOGLE_CLOUD_PROJECT --format="csv(projectNumber)" | tail -n 1)
SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com

gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/resourcemanager.projectIamAdmin

gcloud services enable compute.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable iamcredentials.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable eventarc.googleapis.com
gcloud services enable googleads.googleapis.com
