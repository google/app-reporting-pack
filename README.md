# App Reporting Pack

## Problem statement

Crucial information on App campaigns is scattered across various places in Google Ads UI which makes it harder to get insights into how campaign and assets are performaring.
## Solution

App Reporting Pack fetches all necessary data from Ads API and returns ready-to-use tables that show different aspects of App campaigns performance and settings.

Key pillars of UAC Reporting Pack:

*   Deep Dive Performance Analysis
*   Creatives Insights
*   Campaign Debugging



## Deliverable

Tables in BigQuery that are ready to be used to build
a DataStudio dashboard for App Reporting Pack.

* `asset_performance`
* `creative_excellence`
* `approval_statuses`
* `change_history`
* `performance_grouping_changes`
* `ad_group_network_split`
* `geo_performance`

## Deployment
## Prerequisites

* Google Ads API access - follow documentation on [API authentication](https://github.com/google/ads-api-report-fetcher/blob/main/docs/how-to-authenticate-ads-api.md).
* Python 3.8+
* `google-ads-api-report-fetcher` Python library installed
* Access to repository configured. In order to clone this repository you need to do the following:
    * Visit https://professional-services.googlesource.com/new-password and login with your account
    * Once authenticated please copy all lines in box and paste them in the terminal.

## Installation

In order to run App Reporting Pack please follow the steps outlined below:

* clone this repository `git clone https://professional-services.googlesource.com/solution/uac-reporting-pack`
* configure virtual environment and install a single dependency:
    ```
    python -m venv app-reporting-pack
    source app-reporting-pack/bin/activate
    pip install google-ads-api-report-fetcher
    ```

## Running queries

In order to generate all necessary tables for App Reporting Pack please run `deploy.sh` script:

```shell
bash deploy.sh
```

It will guide you through a series of questions to get all necessary parameters to run the scripts:

* `account_id` - id of Google Ads MCC account (no dashes, 111111111 format)
* `BigQuery project_id` - id of BigQuery project where script will store the data (i.e. `my_project`)
* `BigQuery dataset` - id of BigQuery dataset where script will store the data (i.e. `my_dataset`)
* `start date` - first date from which you want to get performance data (i.e., `2022-01-01`)
* `end date` - last date from which you want to get performance data (i.e., `2022-12-31`)
* `Ads config` - path to `google-ads.yaml` file.


## Disclaimer
This is not an officially supported Google product.

Copyright 2022 Google LLC. This solution, including any related sample code or data, is made available on an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes, and without warranty or representation of any kind. This solution is experimental, unsupported and provided solely for your convenience. Your use of it is subject to your agreements with Google, as applicable, and may constitute a beta feature as defined under those agreements. To the extent that you make any data available to Google in connection with your use of the solution, you represent and warrant that you have all necessary and appropriate rights, consents and permissions to permit Google to use and process that data. By using any portion of this solution, you acknowledge, assume and accept all risks, known and unknown, associated with its usage, including with respect to your deployment of any portion of this solution in your systems, or usage in connection with your business, if at all.

