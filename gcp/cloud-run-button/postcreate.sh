#!/bin/bash
set -e
set -x

echo "Updating Cloud Run service $K_SERVICE"
# ttyd doesn't work on Gen2 environment, so we're updating it to Gen2, additionally increasing request timeout to the maximum allowed
gcloud run services update $K_SERVICE --execution-environment=gen2 --timeout=3600 --max-instances=1 --region=$GOOGLE_CLOUD_REGION
