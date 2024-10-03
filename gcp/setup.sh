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

# ansi colors
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

SETTING_FILE="./settings.ini"
SCRIPT_PATH=$(readlink -f "$0" | xargs dirname)
SETTING_FILE="${SCRIPT_PATH}/settings.ini"

trap _upload_install_log EXIT

# changing the cwd to the script's containing folder so all pathes inside can be local to it
# (important as the script can be called via absolute path and as a nested path)
pushd $SCRIPT_PATH >/dev/null

while :; do
    case $1 in
  -s|--settings)
      shift
      SETTING_FILE=$1
      ;;
  *)
      break
    esac
  shift
done

NAME=$(git config -f $SETTING_FILE config.name)
PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="csv(projectNumber)" | tail -n 1)
USER_EMAIL=$(gcloud config get-value account 2> /dev/null)

APP_CONFIG_FILE=$(eval echo $(git config -f $SETTING_FILE config.config-file))
REPOSITORY=$(eval echo $(git config -f $SETTING_FILE repository.name))
IMAGE_NAME=$(eval echo $(git config -f $SETTING_FILE repository.image))
REPOSITORY_LOCATION=$(git config -f $SETTING_FILE repository.location)
TOPIC=$(eval echo $(git config -f $SETTING_FILE pubsub.topic))

SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com
GCS_BASE_PATH=gs://$PROJECT_ID/$NAME

check_billing() {
  BILLING_ENABLED=$(gcloud beta billing projects describe $PROJECT_ID --format="csv(billingEnabled)" | tail -n 1)
  if [[ "$BILLING_ENABLED" = 'False' ]]
  then
    echo -e "${RED}The project $PROJECT_ID does not have a billing enabled. Please activate billing${NC}"
    exit -1
  fi
}

copy_application_scripts() {
  echo "Copying application files to $GCS_BASE_PATH"
  gsutil rsync -r -x ".*/__pycache__/.*|[.].*" ./../app $GCS_BASE_PATH
}

copy_application_config() {
  echo "Copying configs to $GCS_BASE_PATH"
  gsutil -h "Content-Type:text/plain" cp ./../app/$APP_CONFIG_FILE $GCS_BASE_PATH/
}

copy_googleads_config() {
  echo 'Copying google-ads.yaml to GCS'
  if [[ -f ./../google-ads.yaml ]]; then
    gsutil -h "Content-Type:text/plain" cp ./../google-ads.yaml $GCS_BASE_PATH/google-ads.yaml
  elif [[ -f $HOME/google-ads.yaml ]]; then
    gsutil -h "Content-Type:text/plain" cp $HOME/google-ads.yaml $GCS_BASE_PATH/google-ads.yaml
  else
    echo "Please upload google-ads.yaml"
  fi
}

deploy_files() {
  echo 'Deploying files to GCS'
  if ! gsutil ls gs://$PROJECT_ID > /dev/null 2> /dev/null; then
    echo "Creating GCS bucket gs://$PROJECT_ID"
    gsutil mb -b on gs://$PROJECT_ID
  fi

  echo "Removing existing files at $GCS_BASE_PATH"
  gsutil rm -r $GCS_BASE_PATH/

  copy_application_scripts
  copy_application_config
  copy_googleads_config
}


enable_apis() {
  echo "Enabling APIs"
  gcloud services enable bigquery.googleapis.com
  gcloud services enable compute.googleapis.com
  #gcloud services enable artifactregistry.googleapis.com
  gcloud services enable containerregistry.googleapis.com
  gcloud services enable run.googleapis.com
  gcloud services enable cloudresourcemanager.googleapis.com
  gcloud services enable iamcredentials.googleapis.com
  gcloud services enable cloudbuild.googleapis.com
  gcloud services enable cloudfunctions.googleapis.com
  gcloud services enable eventarc.googleapis.com
  gcloud services enable cloudscheduler.googleapis.com
  gcloud services enable googleads.googleapis.com
}


create_registry() {
  REPO_EXISTS=$(gcloud artifacts repositories list --location=$REPOSITORY_LOCATION --filter="REPOSITORY=projects/'$PROJECT_ID'/locations/'$REPOSITORY_LOCATION'/repositories/'"$REPOSITORY"'" --format="value(REPOSITORY)" 2>/dev/null)
  if [[ ! -n $REPO_EXISTS ]]; then
    echo "Creating a repository in Artifact Registry"
    # repo doesn't exist, creating
    gcloud artifacts repositories create ${REPOSITORY} \
        --repository-format=docker \
        --location=$REPOSITORY_LOCATION
    exitcode=$?
    if [ $exitcode -ne 0 ]; then
      echo -e "${RED}[ ! ] Please upgrade Cloud SDK to the latest version: gcloud components update"
    fi
  fi
}


build_docker_image() {
  echo "Building and pushing Docker image to Artifact Registry"
  gcloud builds submit --config=cloudbuild.yaml --substitutions=_REPOSITORY="docker",_IMAGE="$IMAGE_NAME",_REPOSITORY_LOCATION="$REPOSITORY_LOCATION" ./..
}


build_docker_image_gcr() {
  # NOTE: it's an alternative to build_docker_image if you want to use GCR instead of AR
  echo "Building and pushing Docker image to Container Registry"
  gcloud builds submit --config=cloudbuild-gcr.yaml --substitutions=_IMAGE="$IMAGE_NAME" ./..
}


set_iam_permissions() {
  required_roles="storage.objectViewer artifactregistry.repoAdmin compute.admin monitoring.editor logging.logWriter iam.serviceAccountTokenCreator pubsub.publisher run.invoker"
  echo "Setting up IAM permissions"
  for role in $required_roles; do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member=serviceAccount:$SERVICE_ACCOUNT \
      --role=roles/$role \
      --no-user-output-enabled
  done
}


create_topic() {
  TOPIC_EXISTS=$(gcloud pubsub topics list --filter="name=projects/'$PROJECT_ID'/topics/'$TOPIC'" --format="get(name)")
  if [[ ! -n $TOPIC_EXISTS ]]; then
    gcloud pubsub topics create $TOPIC
  fi
}


deploy_cf() {
  echo "Deploying Cloud Function"
  CF_REGION=$(git config -f $SETTING_FILE function.region)
  CF_NAME=$(eval echo $(git config -f $SETTING_FILE function.name))

  create_topic

  # create env.yaml from env.yaml.template if it doesn't exist
  if [ ! -f ./cloud-functions/create-vm/env.yaml ]; then
    echo "creating env.yaml"
    cp ./cloud-functions/create-vm/env.yaml.template ./cloud-functions/create-vm/env.yaml
  fi
  # initialize env.yaml - environment variables for CF:
  #   - docker image url
  #url="$REPOSITORY_LOCATION-docker.pkg.dev/$PROJECT_ID/docker/$IMAGE_NAME"
  url="gcr.io/$PROJECT_ID/$IMAGE_NAME"
  sed -i'.bak' -e "s|#*[[:space:]]*DOCKER_IMAGE[[:space:]]*:[[:space:]]*.*$|DOCKER_IMAGE: $url|" ./cloud-functions/create-vm/env.yaml
  #   - GCE VM name (base)
  instance=$(eval echo $(git config -f $SETTING_FILE compute.name))
  sed -i'.bak' -e "s|#*[[:space:]]*INSTANCE_NAME[[:space:]]*:[[:space:]]*.*$|INSTANCE_NAME: $instance|" ./cloud-functions/create-vm/env.yaml
  #   - GCE machine type
  machine_type=$(git config -f $SETTING_FILE compute.machine-type)
  sed -i'.bak' -e "s|#*[[:space:]]*MACHINE_TYPE[[:space:]]*:[[:space:]]*.*$|MACHINE_TYPE: $machine_type|" ./cloud-functions/create-vm/env.yaml
  #   - GCE Region
  gce_region=$(git config -f $SETTING_FILE compute.region)
  sed -i'.bak' -e "s|#*[[:space:]]*REGION[[:space:]]*:[[:space:]]*.*$|REGION: $gce_region|" ./cloud-functions/create-vm/env.yaml
  #   - GCE Zone
  gce_zone=$(git config -f $SETTING_FILE compute.zone)
  sed -i'.bak' -e "s|#*[[:space:]]*ZONE[[:space:]]*:[[:space:]]*.*$|ZONE: $gce_zone|" ./cloud-functions/create-vm/env.yaml
  #   - NO_PUBLIC_IP
  no_public_ip=$(git config -f $SETTING_FILE compute.no-public-ip)
  if [[ $no_public_ip == 'true' ]]; then
    if grep -q 'NO_PUBLIC_IP' ./cloud-functions/create-vm/env.yaml; then
      sed -i'.bak' -e "s|^#*[[:space:]]*NO_PUBLIC_IP[[:space:]]*:[[:space:]]*.*$|NO_PUBLIC_IP: 'TRUE'|" ./cloud-functions/create-vm/env.yaml
    else
      echo "" >> ./cloud-functions/create-vm/env.yaml && echo "NO_PUBLIC_IP: 'TRUE'" >> ./cloud-functions/create-vm/env.yaml
    fi
    enable_private_google_access
  else
    sed -i'.bak' -e "s|^NO_PUBLIC_IP[[:space:]]*:|#NO_PUBLIC_IP:|" ./cloud-functions/create-vm/env.yaml
  fi

  # deploy CF (pubsub triggered)
  gcloud functions deploy $CF_NAME \
      --trigger-topic=$TOPIC \
      --entry-point=createInstance \
      --runtime=nodejs18 \
      --timeout=540s \
      --region=$CF_REGION \
      --quiet \
      --gen2 \
      --env-vars-file ./cloud-functions/create-vm/env.yaml \
      --source=./cloud-functions/create-vm/
}


deploy_public_index() {
  echo 'Deploying index.html to GCS'

  if ! gsutil ls gs://$PROJECT_ID-public > /dev/null 2> /dev/null; then
    gsutil mb -b on gs://$PROJECT_ID-public
  fi

  gsutil iam ch -f allUsers:objectViewer gs://${PROJECT_ID}-public 2> /dev/null
  exitcode=$?
  if [ $exitcode -ne 0 ]; then
    echo -e "${RED}[ ! ] Could not add public access to public cloud bucket${NC}"
  else
    GCS_BASE_PATH_PUBLIC=gs://${PROJECT_ID}-public/$NAME
    gsutil -h "Content-Type:text/html" -h "Cache-Control: no-store" cp "${SCRIPT_PATH}/index.html" $GCS_BASE_PATH_PUBLIC/index.html
    if gsutil ls $GCS_BASE_PATH_PUBLIC/dashboard.json >/dev/null 2> /dev/null; then
      gsutil rm $GCS_BASE_PATH_PUBLIC/dashboard.json
    fi
  fi
}


get_run_data() {
  local dashboard_url="$1"
  # arguments for the CF (to be passed via pubsub message or scheduler job's arguments):
  #   * project_id
  #   * machine_type
  #   * service_account
  #   * docker_image - a docker image url, can be CR or AR
  #       gcr.io/$PROJECT_ID/workload
  #       europe-docker.pkg.dev/$PROJECT_ID/docker/workload
  #   * vm - an object with attributes for VM (they will be passed to main.sh via VM's metadata):
  #     * gcs_source_uri
  #     * gcs_base_path_public
  #     * create_dashboard_link
  #     * delete_vm - by default it's TRUE (set inside create-vm CF)
  GCS_BASE_PATH=gs://$PROJECT_ID/$NAME
  GCS_BASE_PATH_PUBLIC=gs://${PROJECT_ID}-public/$NAME

  # NOTE for the commented code:
  # currently deploy_cf target puts a docker image url into env.yaml for CF, so there's no need to pass an image url via arguments,
  # but if you want to support several images simultaneously (e.g. with different tags) then image url can be passed via message as:
  #  "docker_image": "'$REPOSITORY_LOCATION'-docker.pkg.dev/'$PROJECT_ID'/docker/'$IMAGE_NAME'",
  # if you need to prevent VM deletion add this:
  #  "delete_vm": "FALSE"
  data='{
    "vm": {
      "gcs_source_uri": "'$GCS_BASE_PATH'",
      "gcs_base_path_public": "'$GCS_BASE_PATH_PUBLIC'",
      "create_dashboard_url": "'$dashboard_url'",
      "delete_vm": "TRUE"
    }
  }'
  echo $data
}

get_run_data_escaped() {
  local DATA=$(get_run_data)
  ESCAPED_DATA="$(echo "$DATA" | sed 's/"/\\"/g')"
  echo $ESCAPED_DATA
}


start() {
  # example:
  # --message="{\"project_id\":\"$PROJECT_ID\", \"docker_image\":\"europe-docker.pkg.dev/$PROJECT_ID/docker/workload\", \"service_account\":\"$SERVICE_ACCOUNT\"}"

  dashboard_url=$(./../app/scripts/create_dashboard.sh -L --config ./../app/$APP_CONFIG_FILE)

  local DATA=$(get_run_data $dashboard_url)
  echo 'Publishing a pubsub with args: '$DATA
  gcloud pubsub topics publish $TOPIC --message="$DATA"

  # Check if there is a public bucket and index.html and echo the url
  local PUBLIC_URL=$(print_public_gcs_url)/index.html
  local ARP_GOOGLE_GROUP="https://groups.google.com/g/app-reporting-pack-readers-external"
  echo -e "${CYAN}[ * ] Please join Google group to get access to the dashboard - ${GREEN}${ARP_GOOGLE_GROUP}${NC}"
  STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" $PUBLIC_URL)

  if [[ $STATUS_CODE -eq 200 ]]; then
    echo -e "${CYAN}[ * ] To access your new dashboard, click this link - ${GREEN}${PUBLIC_URL}${NC}"
  else
    echo -e "${CYAN}[ * ] Your GCP project does not allow public access.${NC}"
    if [[ -f ./../app/$APP_CONFIG_FILE ]]; then
      echo -e "${CYAN}[ * ] To create your dashboard, click the following link once the installation process completes and all the relevant tables have been created in the DB:"
      echo -e "${GREEN}$dashboard_url${NC}"
    else
      echo -e "${CYAN}[ * ] To create your dashboard, please run the ${GREEN}./scripts/create_dashboard.sh -c $APP_CONFIG_FILE -L${CYAN} shell script once the installation process completes and all the relevant tables have been created in the DB.${NC}"
    fi
  fi
}

print_public_gcs_url() {
  INDEX_PATH="${PROJECT_ID}-public/$NAME"
  PUBLIC_URL="https://storage.googleapis.com/${INDEX_PATH}"
  echo $PUBLIC_URL
}

schedule_run() {
  JOB_NAME=$(eval echo $(git config -f $SETTING_FILE scheduler.name))
  REGION=$(git config -f $SETTING_FILE scheduler.region)
  SCHEDULE=$(git config -f $SETTING_FILE scheduler.schedule)
  SCHEDULE=${SCHEDULE:-"0 0 * * *"} # by default at midnight
  SCHEDULE_TZ=$(git config -f $SETTING_FILE scheduler.schedule-timezone)
  SCHEDULE_TZ=${SCHEDULE_TZ:-"Etc/UTC"}
  local DATA=$(get_run_data)

  delete_schedule

  echo 'Scheduling a job with args: '$DATA
  gcloud scheduler jobs create pubsub $JOB_NAME \
    --schedule="$SCHEDULE" \
    --location=$REGION \
    --topic=$TOPIC \
    --message-body="$DATA" \
    --time-zone=$SCHEDULE_TZ
}


delete_schedule() {
  JOB_NAME=$(eval echo $(git config -f $SETTING_FILE scheduler.name))
  REGION=$(git config -f $SETTING_FILE scheduler.region)

  JOB_EXISTS=$(gcloud scheduler jobs list --location=$REGION --format="value(ID)" --filter="ID=projects/'$PROJECT_ID'/locations/'$REGION'/jobs/'$JOB_NAME'" 2>/dev/null)
  if [[ -n $JOB_EXISTS ]]; then
    echo 'Deleting Cloud Scheduler job '$JOB_NAME
    gcloud scheduler jobs delete $JOB_NAME --location $REGION --quiet
  fi
}

enable_private_google_access() {
  REGION=$(git config -f $SETTING_FILE compute.region)
  gcloud compute networks subnets update default --region=$REGION --enable-private-ip-google-access
}

check_owners() {
  local project_admins=$(gcloud projects get-iam-policy $PROJECT_ID \
    --flatten="bindings" \
    --filter="bindings.role=roles/owner" \
    --format="value(bindings.members[])"
  )
  if [[ ! $project_admins =~ $USER_EMAIL ]]; then
      echo "User $USER_EMAIL does not have admin right to project $PROJECT_ID"
      exit
  fi
}


deploy_all() {
  check_owners
  check_billing
  enable_apis
  set_iam_permissions
  deploy_files
  create_registry
  build_docker_image
  #build_docker_image_gcr  # Container Registry is deprecated
  deploy_cf
  schedule_run
}

_upload_install_log() {
  if [[ -f "/tmp/${NAME}_installer.log" ]]; then
    gsutil cp /tmp/${NAME}_installer.log gs://$PROJECT_ID/$NAME/
    rm "/tmp/${NAME}_installer.log"
  fi
}

_list_functions() {
  # list all functions in this file not starting with "_"
  declare -F | awk '{print $3}' | grep -v "^_"
}


if [[ $# -eq 0 ]]; then
  _list_functions
else
  for i in "$@"; do
    if declare -F "$i" > /dev/null; then
      "$i" 2>&1 | tee -a /tmp/${NAME}_installer.log
      exitcode=$?
      if [ $exitcode -ne 0 ]; then
        echo "Breaking script as command '$i' failed"
        exit $exitcode
      fi
    else
      echo -e "\033[0;31mFunction '$i' does not exist.\033[0m"
    fi
  done
fi

popd > /dev/null
