# App Reporting Pack

##### Centralized platform and dashboard for Google Ads App campaign data

Crucial information on App campaigns is scattered across various places in Google Ads UI which makes it harder to get insights into how campaign and assets perform.
App Reporting Pack fetches all necessary data from Ads API and creates a centralized dashboard showing different aspects of App campaign's performance and settings. All data is stored in BigQuery tables that can be used for any other need the client might have.


## Deliverables

1. A centralized dashboard with deep app campaign and assets performance views
2. The following data tables in BigQuery that can be used independently:

* `asset_performance`
* `creative_excellence`
* `approval_statuses`
* `change_history`
* `performance_grouping_history`
* `ad_group_network_split`
* `geo_performance`
* `cannibalization`

## Prerequisites

1. [A Google Ads Developer token](https://developers.google.com/google-ads/api/docs/first-call/dev-token#:~:text=A%20developer%20token%20from%20Google,SETTINGS%20%3E%20SETUP%20%3E%20API%20Center.)

1. A new GCP project with billing account attached

1. Membership in `app-reporting-pack-readers-external` Google group (join [here](https://groups.google.com/a/google.com/g/app-reporting-pack-readers-external))

1. Credentials for Google Ads API access - `google-ads.yaml`.
   See details here - https://github.com/google/ads-api-report-fetcher/blob/main/docs/how-to-authenticate-ads-api.md
   Normally you need OAuth2 credentials (Client ID, Client Secret), a Google Ads developer token and a refresh token.


## Setup

1. Click the big blue button to deploy:

   [![Run on Google Cloud](https://deploy.cloud.run/button.svg)](https://deploy.cloud.run)

1. Select your GCP project and choose your region

1. When prompted, paste in your client ID, client secret, refresh token, developer token and MCC ID

1. Wait for the deployment to finish. Once finished you will be given your ***URL***

1. Click on "Run Queries" to manually run the queries. The queries are scheduled to run daily automatically.

1. Once the tables are created click "Create Dashboard". This will create your own private copy of the App Reporting Pack dashboard. Change your dashboard's name and save it's URL or bookmark it.


## Installation alternatives

### Prerequisites for alternative installation methods

* Google Ads API access and [google-ads.yaml](https://github.com/google/ads-api-report-fetcher/blob/main/docs/how-to-authenticate-ads-api.md#setting-up-using-google-adsyaml) file - follow documentation on [API authentication](https://github.com/google/ads-api-report-fetcher/blob/main/docs/how-to-authenticate-ads-api.md).
* Python 3.8+
* [Service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating) created and [service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) downloaded in order to write data to BigQuery.
    * Once you downloaded service account key export it as an environmental variable
        ```
        export GOOGLE_APPLICATION_CREDENTIALS=path/to/service_account.json
        ```

    * If authenticating via service account is not possible you can authenticate with the following command:
         ```
         gcloud auth application-default login
         ```

### Running queries locally

In order to run App Reporting Pack locally please follow the steps outlined below:

* clone this repository
    ```
    git clone https://github.com/google/app-reporting-pack
    cd app-reporting-pack
    ```
* (Recommended) configure virtual environment if you starting testing the solution:
    ```
    sudo apt-get install python3-venv
    python3 -m venv app-reporting-pack
    source app-reporting-pack/bin/activate
    ```
* Make sure that that pip is updated to the latest version:
    ```
    python3 -m pip install --upgrade pip
    ```
* install dependencies:
    ```
    pip install -r requirements.in
    ```
Please run `run-local.sh` script in a terminal to generate all necessary tables for App Reporting Pack:

```shell
bash ./run-local.sh
```

It will guide you through a series of questions to get all necessary parameters to run the scripts:

* `account_id` - id of Google Ads MCC account (no dashes, 111111111 format)
* `BigQuery project_id` - id of BigQuery project where script will store the data (i.e. `my_project`)
* `BigQuery dataset` - id of BigQuery dataset where script will store the data (i.e. `my_dataset`)
* `start date` - first date from which you want to get performance data (i.e., `2022-01-01`). Relative dates are supported [see more](https://github.com/google/ads-api-report-fetcher#dynamic-dates).
* `end date` - last date from which you want to get performance data (i.e., `2022-12-31`). Relative dates are supported [see more](https://github.com/google/ads-api-report-fetcher#dynamic-dates).
* `Ads config` - path to `google-ads.yaml` file.
* `Video orientation mode` - how to get video orientation for video assets - from YouTube (`youtube` mode, [learn more on authentication](docs/setup-youtube-api-to-fetch-video-orientation.md)), from asset name (`regex` mode) or use placeholders (`placeholders` mode).

After the initial run of `run-local.sh` command it will generate `app_reporting_pack.yaml` config file with all necessary information used for future runs.
When you run `bash run-local.sh` next time it will automatically pick up created configuration.

#### Schedule running `run-local.sh` as a cronjob

When running `run-local.sh` scripts you can specify two options which are useful when running queries periodically (i.e. as a cron job):

* `-c <config>`- path to `app_reporting_pack.yaml` config file. Comes handy when you have multiple config files or the configuration is located outside of current folder.
* `-q` - skips all confirmation prompts and starts running scripts based on config file.

> `run-local.sh` support `--legacy` command line flag which is used to generate dashboard in the format compatible with existing dashboard.
> If you're migrating existing datasources `--legacy` option might be extremely handy.

If you installed all requirements in a virtual environment you can use the trick below to run the proper cronjob:

```
* 1 * * * /usr/bin/env bash -c "source /path/to/your/venv/bin/activate && bash /path/to/app-reporting-pack/run-local.sh -c /path/to/app_reporting_pack.yaml -g /path/to/google-ads.yaml -q"
```

This command will execute App Reporting Pack queries every day at 1 AM.


### Running in a Docker container

You can run App Reporting Pack queries inside a Docker container.

```
sudo docker run \
    -v /path/to/google-ads.yaml:/google-ads.yaml \
    -v /path/to/service_account.json:/app/service_account.json \
    -v /path/to/app_reporting_pack.yaml:/app_reporting_pack.yaml \
    ghcr.io/google/app-reporting-pack \
    -g google-ads.yaml -c app_reporting_pack.yaml --legacy --backfill
```

> Don't forget to change /path/to/google-ads.yaml and /path/to/service_account.json with valid paths.

You can provide configs as remote (for example Google Cloud Storage).
In that case you don't need to mount `google-ads.yaml` and `app_reporting_pack.yaml`
configs into the container:
```
sudo docker run \
    -v /path/to/service_account.json:/app/service_account.json \
    ghcr.io/google/app-reporting-pack \
    -g gs://project_name/google-ads.yaml \
    -c gs://project_name/app_reporting_pack.yaml \
    --legacy --backfill
```

### Running in Compute Engine with Docker
First follow the instructions for manual installation above. In the end you will need to have `config.yaml`
`google-ads.yaml` files. After that you are ready to install.
Go to gcp folder and run the following command:
```
./setup.sh deploy_all
```
This will do the followings:
* enable APIs
* grant required IAM permissions
* create a repository in Artifact Repository
* build a Docker image (using `gcp/workload-vm/Dockerfile` file)
* publish the image into the repository
* deploy Cloud Function `create-vm` (from gcp/cloud-functions/create-vm/) (using environment variables in env.yaml file)
* deploy configs to GCS (config.yaml and google-ads.yaml) (to a bucket with a name of current GCP project id and 'arp' subfolder)
* create a Scheduler job for publishing a pubsub message with arguments for the CF

What happens when a pubsub message published:
* the Cloud Function 'create-vm' get a message with arguments and create a virtual machine based a Docker container from the Docker image built during the installation
* the VM on startup parses the arguments from the CF (via VM's attributes) and execute ARP in quite the same way as it executes locally (using `run-local.sh`). Additionally the VM's entrypoint script deletes the virtual machine upon completion of the run-local.sh.


### Dashboard Replication

Once queries ran successfully you can proceed with dashboard replication.\
Run the following command in the terminal to create a copy of the dashboard:

```
bash scripts/create_dashboard.sh -c app_reporting_pack.yaml
```

For more details on dashboard please refer to [how-to-replicate-app-reporting-pack](docs/how-to-replicate-app-reporting-pack.md) document.

## Disclaimer
This is not an officially supported Google product.
