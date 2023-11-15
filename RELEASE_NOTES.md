# App Reporting Pack Release Notes

## Version 1.3.0 [2023-11-01]
> Breaking changes. Please re-create your existing version of your App Reporting Pack.

### Dashboard

  * Added links to Google Ads Account/Campaigns/Ad Groups
      * all datasources contain `account`, `campaign` and optionally `ad_group` fields which are hyperlinks to Google Ads UI.
  * Fixed bug with incorrect assets cohort calculation (Page 4 *Asset Reporting* - chart *Cohort Analysis*)
    * `inapps_30_day` is used as a base for calculating the `inapp_N_day_%` metric which results in a correct chart view.

### Data

* Add support for incremental saving of performance data and performing initial load.
* Make backfilling of bid budgets and asset cohorts a default behaviour
* Simplify installation process:
    * Add default values during the installation - simply accept them and proceed with the installation in two clicks.
    * Check whether `google-ads.yaml` is correct before proceeding to the installation.
    * Various improvements to installing solution to Google Cloud
* Add modularity to the app
    * You can run `bash run-local.sh --modules disapprovals` to get only data on disapprovals; by default `core,assets,disapprovals,ios_skan` modules are fetched, you can explore modules at `app` folder.


## Version 1.2.0 [2023-07-31]

### Dashboard

  New App Reporting Pack with iOS SKAN support  - [dashboard](https://lookerstudio.google.com/c/u/0/reporting/3f042b13-f767-4195-b092-35b94e0b430c/page/0hcO)

### Data

* Added iOS SKAN Support
    * Adeed new table `ios_skan_decoder` and new datasource `skan_decoder` in Looker Studio dashboard
    * Users can specify iOS SKAN decoder schema to improve their SKAN reporting
      [how to specify iOS SKAN decoder schema](docs/how-to-specify-ios-skan-decoder-schema.md)
    * SKAN postbacks are added to `change_history` dataset
* Improved process of installing the solution
    * Improve documentation on [getting video orientation](docs/how-to-get-video-orientations-for-assets.md)
* Fixed bug with empty conversion lag adjustment

## Version 1.1.0 [2022-11-23]

Base version

* [Dashboard](https://lookerstudio.google.com/c/u/0/reporting/187f1f41-16bc-434d-8437-7988bed6e8b9/page/0hcO) for version 1.1.0
