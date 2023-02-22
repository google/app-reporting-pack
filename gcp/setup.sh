TOPIC=run-arp
REPOSITORY=$(git config -f ./settings.ini repository.name)
IMAGE_NAME=$(git config -f ./settings.ini repository.image)
REPOSITORY_LOCATION=$(git config -f ./settings.ini repository.location)
CF_REGION=$(git config -f ./settings.ini function.region)
CF_NAME=$(git config -f ./settings.ini function.name)

PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="csv(projectNumber)" | tail -n 1)
SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com

enable_apis() {
  gcloud services enable compute.googleapis.com
  gcloud services enable artifactregistry.googleapis.com
  gcloud services enable run.googleapis.com
  gcloud services enable cloudresourcemanager.googleapis.com
  gcloud services enable iamcredentials.googleapis.com
  gcloud services enable cloudbuild.googleapis.com
  gcloud services enable cloudfunctions.googleapis.com
}


# create an Artifact Repository
create_registry() {
  gcloud artifacts repositories create $REPOSITORY \
      --repository-format=Docker \
      --location=$REPOSITORY_LOCATION
}


build_docker_image() {
  # build and push Docker image to Artifact Registry
  gcloud builds submit --config=cloudbuild.yaml --substitutions=_REPOSITORY="docker",_IMAGE="$IMAGE_NAME",_REPOSITORY_LOCATION="$REPOSITORY_LOCATION" ./.. 
}


build_docker_image_gcr() {
  # build and push Docker image to Container Registry
  gcloud builds submit --config=cloudbuild-gcr.yaml --substitutions=_IMAGE="workload" ./workload-vm
}


set_iam_permissions() {
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/storage.objectViewer
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/artifactregistry.repoAdmin
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/compute.admin
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/monitoring.editor
}


deploy_cf() {
  gcloud pubsub topics create $TOPIC

  # create env.yaml from env.yaml.template if it doesn't exist
  if [ ! -f ./cloud-functions/create-vm/env.yaml ]; then
    echo "creating env.yaml"
    cp ./cloud-functions/create-vm/env.yaml.template ./cloud-functions/create-vm/env.yaml
  fi
  # initialize env.yaml - environment variables for CF
  url="$REPOSITORY_LOCATION-docker.pkg.dev/$PROJECT_ID/docker/$IMAGE_NAME"
  sed -i'.original' -e "s|#*[[:space:]]*DOCKER_IMAGE[[:space:]]*:[[:space:]]*.*$|DOCKER_IMAGE: $url|" ./cloud-functions/create-vm/env.yaml

  # deploy CF
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


deploy_config() {
  echo 'Deploying config to GCS'
  GCS_BASE_PATH=gs://$PROJECT_ID/arp
  gsutil -h "Content-Type:text/plain" -m cp -R ./config.json $GCS_BASE_PATH/arp/config.json
}


start() {
  # args for the cloud function (create-vm) passed via pub/sub event:
  #   * project_id - 
  #   * docker_image - a docker image url, can be CR or AR
  #       gcr.io/$PROJECT_ID/workload
  #       europe-docker.pkg.dev/$PROJECT_ID/docker/workload
  #   * service_account 
  # --message="{\"project_id\":\"$PROJECT_ID\", \"docker_image\":\"europe-docker.pkg.dev/$PROJECT_ID/docker/workload\", \"service_account\":\"$SERVICE_ACCOUNT\"}"
  gcloud pubsub topics publish $TOPIC --message="{\"docker_image\":\"$REPOSITORY_LOCATION-docker.pkg.dev/$PROJECT_ID/docker/$IMAGE_NAME\"}"
}

schedule_run() {
  JOB_NAME=$(git config -f ./settings.ini scheduler.name)
  SCHEDULE=$(git config -f ./settings.ini scheduler.schedule)
  SCHEDULE=${SCHEDULE:-"0 0 * * *"} # by default at midnight

  gcloud scheduler jobs delete $JOB_NAME --location $REGION --quiet

  gcloud scheduler jobs create pubsub $JOB_NAME \
    --schedule="$SCHEDULE" \
    --location=$REGION \
    --topic=$TOPIC \
    --message-body="{\"argument\": \"$ESCAPED_WF_DATA\"}" \
    --oauth-service-account-email="$SERVICE_ACCOUNT" \
    --time-zone="Etc/UTC"
}

deploy_all() {
  enable_apis
  set_iam_permissions
  create_registry
  build_docker_image
  deploy_cf
#  deploy_config
}


for i in "$@"; do
    "$i"
done
