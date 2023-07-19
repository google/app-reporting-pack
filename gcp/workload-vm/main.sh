#!/bin/bash

LOG_NAME=arp-vm

echo "Starting entrypoint script"

# Fetch GCP project_id from Metadata service and set it via gcloud
project_id=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/project/project-id -s)
echo "Detected project id from metadata server: $project_id"
gcloud config set project $project_id

# Fetch gcs uris fro the current instance metadata
gcs_source_uri=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/attributes/gcs_source_uri -s --fail)

gcloud logging write $LOG_NAME "[$(hostname)] Starting ARP application (gcs_source_uri: $gcs_source_uri)"

# fetch ARP files from GCS
if [[ -n $gcs_source_uri ]]; then
  folder_name=$(basename "$gcs_source_uri")
  gsutil -m cp -R $gcs_source_uri .
  mv "$folder_name/*" .
fi

# run ARP
# TODO: --backfill?
./run-local.sh --quiet --config app_reporting_pack.yaml --google-ads-config google-ads.yaml --legacy
exitcode=$?

if [ $exitcode -ne 0 ]; then
  gcloud logging write $LOG_NAME "[$(hostname)] ARP application has finished execution with an error ($exitcode)" --severity ERROR
  # TODO: send the error somewhere
else
  gcloud logging write $LOG_NAME "[$(hostname)] ARP application has finished execution successfully"
fi

# Check if index.html exists in the bucket. If so - create and upload dashboard.json
gcs_base_path_public=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/attributes/gcs_base_path_public -s --fail)
if [[ -n gcs_base_path_public ]]; then
  # TODO: if run-local.sh failed we shouldn't create dashboard_url
  if gsutil ls $gcs_base_path_public/index.html >/dev/null 2>&1; then
    chmod +x ./scripts/create_dashboard.sh
    dashboard_url=$(./scripts/create_dashboard.sh -L --config app_reporting_pack.yaml)
    echo "Created dashboard cloning url: $dashboard_url"
    echo "{\"dashboardUrl\":\"$dashboard_url\"}" > dashboard.json
    gsutil -h "Content-Type:application/json" -h "Cache-Control: no-store" cp dashboard.json $gcs_base_path_public/dashboard.json
  fi
fi 

# Delete the VM (fetch a custom metadata key, it can be absent, so returns 404 - handling it with --fail options)
delete_vm=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/attributes/delete_vm -s --fail)
echo "Delete VM: $delete_vm"
if [[ "$delete_vm" = 'TRUE' ]]; then
    gcp_zone=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/zone -s | cut -d/ -f4)
    gcloud compute instances delete $(hostname) --zone ${gcp_zone} -q
fi

gcloud logging write $LOG_NAME "[$(hostname)] Docker entrypoint script completed"
echo "Entrypoint script completed"