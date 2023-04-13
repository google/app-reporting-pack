#!/bin/bash
#set -e
NC='\033[0m' # No Color
Black='\033[0;30m'        # Black
Red='\033[0;31m'          # Red
Green='\033[0;32m'        # Green
Yellow='\033[0;33m'       # Yellow
Blue='\033[0;34m'         # Blue
Purple='\033[0;35m'       # Purple
Cyan='\033[0;36m'         # Cyan
White='\033[0;37m'        # White

install() {
  echo -e "${Cyan}Generating App Reporting pack configuration...${NC}"
  # generate ARP config and optionally google-ads.yaml (if it wasn't uploaded in cloud shell vm)
  RUNNING_IN_GCE=true
  export RUNNING_IN_GCE   # signaling to run-local.sh that we're runnign inside GCE (there'll be less questions)
  ./run-local.sh --generate-config-only
  echo -e "${Cyan}Starting Deploying Cloud components...${NC}"
  # deploy solution
  ./gcp/setup.sh deploy_public_index deploy_all start
  bash
}

if [[ -f ./app_reporting_pack.yaml ]]; then
  # found app_reporting_pack.yaml from previous run
  echo -e "${White}It seems you have deployed the solution already.${NC}"
  # when installation AND first run complete then there will be a dashboard.json file on public GCS
  public_gcs_url=$(./gcp/setup.sh print_public_gcs_url)
  if curl $public_gcs_url/dashboard.json --fail 2>/dev/null; then
    echo -e "If you haven't already, use that url for dashboard cloning"
    echo -n -e "${Red}Would you like to delete the current Cloud Run service (it's needed only for installation) (Y/n): ${NC}"
    read -r SHUTDOWN
    SHUTDOWN=${SHUTDOWN:-y}
    if [[ $SHUTDOWN = 'Y' || $SHUTDOWN = 'y' ]]; then
      REGION=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/region -s --fail)
      REGION=$(basename $REGION)
      gcloud run services delete $K_SERVICE --region $REGION --quiet
    else
      echo -n "Would you like to restart installation? (Y/n): "
      read -r REPEAT_INSTALLATION
      REPEAT_INSTALLATION=${REPEAT_INSTALLATION:-y}
      if [[ $REPEAT_INSTALLATION =~ ^[Yy]$ ]]; then
        install
      else
        echo "This is the end"
        bash
        exit
      fi
    fi
  else
    # installation started but dashboard cloning url hasn't generated yet
    echo "Please track progress here: $public_gcs_url/index.html"
    bash
    exit
  fi
else
  install
fi
