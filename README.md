# UAC Reporting Pack 

UAC Reporting Pack is a Data Studio based solution that shows different aspects
of App campaigns performance and settings.

Key pillars of UAC Reporting Pack:

*   Deep Dive Performance Analysis
*   Creatives Insights
*   Campaign Debugging

## Prerequisites

* Create virtual environment `python -m venv uac-reporting-pack` and activate it with `source uac-reporting-pack/bin/activate`.
* Install `google-ads-api-report-fetcher` library with `pip install google-ads-api-report-fetcher`.
* Follow documentation on [API authentication](https://github.com/google/ads-api-reports-fetcher#getting-started) to generate `google-ads.yaml` file;
    if you already have such file you may skip this step.


## Running queries

1. Specify enviromental variables

```
export CUSTOMER_ID=
export BQ_PROJECT=
export BQ_DATASET=
export START_DATE=
export END_DATE=
```

`START_DATE` and `END_DATE` should be specified in `YYYY-MM-DD` format (i.e. 2022-01-01).
`CUSTOMER_ID` should be specifed in `1234567890` format (no dashes between digits).

2. Run `fetch-reports` command to fetch Google Ads data and store them in BigQuery

```
fetch-reports google_ads_queries/*/*.sql \
    --account=$CUSTOMER_ID \
    --output=bq \
    --bq.project=$BQ_PROJECT \
    --bq.dataset=$BQ_DATASET \
    --sql.start_date=$START_DATE \
    --sql.end_date=$END_DATE \
    --ads-config=path/to/google-ads.yaml
```

3. Run `post-process-queries` command to prepare tables in BigQuery based on data
fetched by `fetch-reports` command.

```
post-process-queries bq_queries/*.sql \
    --bq.project=$BQ_PROJECT \
    --bq.dataset=$BQ_DATASET \
```

## Disclaimer
This is not an officially supported Google product.

Copyright 2022 Google LLC. This solution, including any related sample code or data, is made available on an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes, and without warranty or representation of any kind. This solution is experimental, unsupported and provided solely for your convenience. Your use of it is subject to your agreements with Google, as applicable, and may constitute a beta feature as defined under those agreements. To the extent that you make any data available to Google in connection with your use of the solution, you represent and warrant that you have all necessary and appropriate rights, consents and permissions to permit Google to use and process that data. By using any portion of this solution, you acknowledge, assume and accept all risks, known and unknown, associated with its usage, including with respect to your deployment of any portion of this solution in your systems, or usage in connection with your business, if at all.

