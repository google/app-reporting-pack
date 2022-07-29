#!/bin/bash
ads_queries=$1
bq_queries=$2
gaarf $ads_queries -c=config.yaml --ads-config=google-ads.yaml
python3 conv_lag_adjustment.py -c=config.yaml
gaarf-bq $bq_queries/views_and_functions/*.sql -c=config.yaml
gaarf-bq $bq_queries/snapshots/*.sql -c=config.yaml
gaarf-bq $bq_queries/*.sql -c=config.yaml
gaarf-bq $bq_queries/legacy_views/*.sql  -c=config.yaml

