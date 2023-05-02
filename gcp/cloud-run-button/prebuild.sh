#!/bin/bash
set -e
#set -x

cd ../..

NC='\033[0m' # No Color
RED='\033[0;31m' # Red Color
CYAN='\033[0;36m' # Cyan
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
    echo -n "Please upload google-ads.yaml and enter 'Y' or enter 'N' to skip: "
    read -r USE_GOOGLE_ADS_CONFIG
  else
    break
  fi
done

gcloud config set project $GOOGLE_CLOUD_PROJECT

./gcp/install.sh

echo -e "${CYAN}Please ignore all output below${WHITE}"
