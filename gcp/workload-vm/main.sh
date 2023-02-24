#!/bin/bash
set -e  # break at first non-zero exitcode

LOG_NAME=arp-vm

echo "Starting entrypoint script"

# Fetch GCP project_id from Metadata service and set it via gcloud
project_id=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/project/project-id -s)
echo "Detected project id from metadata server: $project_id"
gcloud config set project $project_id

# Fetch config uris fro the current instance metadata
config_uri=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/attributes/config_uri -s --fail)
if [ -z "$config_uri" ]; then
  config_uri="config.yaml" # by default we assume a local config (inside the current container)
  echo $config_uri
fi
ads_config_uri=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/attributes/ads_config_uri -s --fail)
if [ -z "$ads_config_uri" ]; then
  ads_config_uri="google-ads.yaml"
  echo $ads_config_uri
fi

gcloud logging write $LOG_NAME "[$(hostname)] Starting ARP application (config: $config_uri, google-ads-config: $ads_config_uri)"

# run ARP
./run-docker.sh "google_ads_queries/*/*.sql" "bq_queries" "$ads_config_uri" "$config_uri"
#./run-local.sh --quiet --config $config_uri --google-ads-config $ads_config_uri 

gcloud logging write $LOG_NAME "[$(hostname)] ARP application has finished execution"


# Delete the VM (fetch a custom metadata key, it can be absent, so returns 404 - handling it with --fail options)
delete_vm=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/attributes/delete_vm -s --fail)
echo "Delete VM: $delete_vm"
if [[ "$delete_vm" = 'TRUE' ]]; then
    gcp_zone=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/zone -s | cut -d/ -f4)
    gcloud compute instances delete $(hostname) --zone ${gcp_zone}
fi

gcloud logging write $LOG_NAME "[$(hostname)] Docker entrypoint script completed"
echo "Entrypoint script completed"