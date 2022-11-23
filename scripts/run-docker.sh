#!/bin/bash
echo "run-docker script triggered"
ads_queries=$1
bq_queries=$2
ads_yaml=$3
gaarf $ads_queries -c=/config.yaml --ads-config=$ads_yaml
python3 conv_lag_adjustment.py -c=/config.yaml --ads-config=$ads_yaml
gaarf-bq $bq_queries/snapshots/*.sql -c=/config.yaml
gaarf-bq $bq_queries/views_and_functions/*.sql -c=/config.yaml
gaarf-bq $bq_queries/*.sql -c=/config.yaml
gaarf-bq $bq_queries/legacy_views/*.sql  -c=/config.yaml

