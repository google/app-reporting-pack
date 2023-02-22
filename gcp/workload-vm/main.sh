#!/bin/bash

APP_NAME=arp-vm

echo "Starting entrypoint script"

# Fetch GCP project_id from Metadata service
project_id=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/project/project-id -s)
echo "Detected project id from metadata server: $project_id"
gcloud config set project $project_id

# Fetch the current instance metadata (config_uri and delete_vm)
config_uri=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/attributes/config_uri -s --fail)
if [[ "$config_uri" = '' ]]; then
    config_uri="config.yaml" # by default we assume a local config (inside the current container)
fi
echo "Config uri to use: $config_uri"

gcloud logging write $APP_NAME "[$(hostname)] Starting ARP application (config uri: $config_uri)"

# run ARP
./run-docker.sh "google_ads_queries/*/*.sql" "bq_queries" "google-ads.yaml" "config.yaml"

gcloud logging write $APP_NAME "[$(hostname)] ARP application has finished execution"

echo "Entrypoint script execution finished"

# Delete the VM (fetch a custom metadata key, it can be absent, so returns 404 - handling it with --fail options)
delete_vm=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/attributes/delete_vm -s --fail)
echo "Delete VM: $delete_vm"
if [[ "$delete_vm" = 'TRUE' ]]; then
    gcp_zone=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/zone -s | cut -d/ -f4)
    gcloud compute instances delete $(hostname) --zone ${gcp_zone}
fi

gcloud logging write $APP_NAME "[$(hostname)] Docker entrypoint script completed"