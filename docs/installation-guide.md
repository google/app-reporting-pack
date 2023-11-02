# App Reporting Pack deployment guide

## How ARP works
ARP is a tool allowing fetching and accumulating data from various Google Ads reports. It can fetch ad group and asset performance data (daily clicks, impressions, conversions, etc), snapshots of campaign or ad group settings (target bids, budgets, eligibility etc), ad disapproval statuses and other information. Having all that data stored in a local database users can do advanced analysis such as cohort analysis, tracking changes impact or ad disapproval.

Different types of Google Ads data require different approaches:
- Performance data (impressions, clicks, conversions) can be extracted at any moment for any period of time in the past (up to X days). The conversion data can not be simply accumulated, meaning that one cannot just extract conversions for the previous day, save them, the next day repeat it and save again, etc. In order to see correct conversion numbers it’s required to always fetch data for an interval not shorter than the conversion window in Google Ads. We call this time interval the **Reporting Window**.
- Campaigns/ad groups/ads/creatives settings and statuses are snapshots. In order to have continuous statistics without gaps, that information should be extracted from Google Ads on a daily basis (although some of the historical settings can be restored from Google Ads).
- Cohort performance is calculated based on the daily performance snapshots. If you miss a day and don’t extract performance data then there will be a gap in cohort performance which can’t be reliably restored.
	![initial_load](src/initial_load.png)

ARP configuration tips:

* Choose the right reporting window length. It should be longer than the longest conversion window in Google Ads, but if it’s too long it will result in extracting too much unnecessary data which can be bad for large Google Ads accounts. It’s not recommended to change the reporting window after ARP starts collecting data as it can cause data losing.
* If you deploy ARP and need to extract historical performance data then you can provide the **Initial load date**. If you do not provide the initial load date then the ARP will accumulate data starting from the current date minus reporting window.
* When ARP is run for the first time it will be able to restore the historical settings only for the last 30 days. If the selected initial load date is earlier than 30 days back then you will see a gap in target ROAS, CPA and change history. Cohort performance will not be available for the period prior to the deployment.


## Step-by-step deployment

1 . Choose one of the deployment methods:

* Click on Run on Google Cloud button
* execute `bash app/run-local.sh` locally
* run  `bash gcp/install.sh` in Google Cloud Shell
	![run_local](src/run_local.png)

2. The script will offer to deploy ARP with default settings.
	![default](src/default.png)
    - **account_id** - Google Ads account or MCC Id provided in the google-ads.yaml.
    - **BigQuery project_id** - current GCP project where the user is authenticated.
    - **BigQuery dataset** - dataset in the current project where data will be stored.
    - **Reporting window** - how much data will be daily extracted from Google Ads. Reporting window should be of reasonable size but not shorter than the conversion window in Google Ads.
    - **Initial load date** - by default historical data will not be extracted. The Initial load date will be the current date - the reporting window.
    - **Ads config path** - path to the provided google-ads.yaml.
    - **Cohorts** - a list of offsets in days since the attributed interaction. ARP calculates how many conversions occur on 1st, 2nd, 5th day after an interaction with an ad.
    - **Video parsing mode** - ARP needs extra effort to obtain dimensions of video ads. It can be extracted either from a YouTube channel or by parsing video asset names (in this case the dimensions should be a part of asset naming convention). By default ARP doesn’t extract video dimensions. Video parsing mode can be changed any time later.

The user can either accept the default setting or press N to configure each parameter. Even though a user accepts the default settings they will be additionally asked to configure optional SKAN schema for SKAN in-app events decoding.

3. If a user has opted out from the default settings they will be asked to set it up one by one. In most cases a default value will be offered (shown in parenthesis). In order to accept the default value a user can press Enter without typing anything.

    ![configure](src/configure.png)

    *Reporting window and initial load date:*
    ![initial_load_date](src/initial_load_date.png)

    The initial load date is not validated. Entering a date in the future or a date earlier than CURRENT_DATE - REPORTING_WINDOW may result in runtime error or incorrect data.

    *Cohorts:*
    ![cohorts](src/cohorts.png)

    Enter a comma separated list of numbers. If non-default values are used then some changes in the Looker Studio dashboard may be required, as the reports are configured to be used with the default cohorts.

    *Video orientation:*

    There are 2 possible scenario:
    * If you have a consistent naming for your videos you can extract width and height from asset names.
    If getting data from video names is impossible you can always fetch video dimensions directly from YouTube Data API (requires authentication in YouTube).
    * In the 1st case asset names should have a static naming convention similar to one on the screenshot my_video_1280x720_concept_name.mp4. It’s required that the dimension should always be at the same position in the name.
    A user will be asked the delimiter character, zero based dimension position number and width and height delimiter character.

    ![video_orientation](src/video_orientation.png)

    See [Get video orientation from YouTube Data API](how-to-get-video-orientation-for-assets.md)  document for more information about YouTube API setup.

    *SKAN schema:*

    SKAN Schema is optional, it’s required if you want to have decoded SKAN in-app conversions in the report. The schema is a BigQuery table in any dataset, not necessarily the dataset with the ARP data. The name of the table can also be arbitrary. After execution of the script the schema will be copied to the ARP dataset. The script expects the schema as a fully qualified table name like `project.dataset.table_name`.

    ![skan](src/skan.png)

    For more information on the SKAN schema set up see [SKAN ARP guide](how-to-specify-ios-skan-schema.md).

## Advanced setup

ARP can be configured manually by editing the config.yaml file. Usually config.yaml is created by the deployment script based on the answers on the questions, but it’s also possible to create it from scratch or modify the existing file.

Sample `config.yaml`:

```
gaarf:
  output: bq
  bq:
    project: YOUR-BQ-PROJECT
    dataset: arp
  api_version: '14'
  account:
  - 'YOUR_MCC_ID'
  customer_ids_query: SELECT customer.id FROM campaign WHERE campaign.advertising_channel_type
    = "MULTI_CHANNEL"
  params:
    macro:
      start_date: :YYYYMMDD-91
      end_date: :YYYYMMDD-1
      initial_load_date: '2023-01-01'
gaarf-bq:
  project: YOUR-BQ-PROJECT
  params:
    macro:
      bq_dataset: arp
      target_dataset: arp_output
      legacy_dataset: arp_legacy
      skan_schema_input_table: YOUR_PROJECT.YOUR_DATASET.YOUR_SKAN_SCHEMA_TABLE
    template:
      cohort_days: 1,3,5,7,14,30
      has_skan: 'true'
      incremental: 'true'
scripts:
  video_orientation:
    mode: regex
    element_delimiter: _
    orientation_position: '3'
    orientation_delimiter: x
  skan_mode:
    mode: placeholders
backfill: true
incremental: true
legacy: true
```

Important parameters:
* **api_version** - current Google Ads API version. It’s recommended to keep it up to date and use the most recent version. Please follow ARP updates.
* **start_date** - beginning of the reporting window. Usually it’s a macro :YYYYMMDD-N, where N is the length of the reporting window. It’s not recommended to to change start date as it may cause data disruption
* **end_date** - end of the reporting window. By default it’s the previous day YYYYMMDD-1
* **dataset** - name of the main dataset. ARP stores all the intermediate tables there
* **target_dataset** - name of the dataset where the output tables are stored
* **legacy_dataset** - name of the dataset for views for backward compatibility with older versions of ARP
* **skan_schema_input_table** - source SKAN schema name. If the schema wasn’t provided during the initial deployment, it can be set here
* **skan_mode.mode** - should be “table” if skan_schema_input_table is provided. Otherwise it’s “placeholder”
* **has_skan** - whether to extract SKAN reports. By default it’s true
* **incremental** - whether to accumulate historical performance. By default it’s true. If set to false, then ARP will not preserve data for the dates earlier than the beginning of the reporting window. In this case it’s recommended to select a wider reporting window.

