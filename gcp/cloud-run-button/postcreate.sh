#!/bin/bash
set -e
#set -x

echo "Deleting Cloud Run service $K_SERVICE"

gcloud run services delete $K_SERVICE --region $GOOGLE_CLOUD_REGION --quiet
