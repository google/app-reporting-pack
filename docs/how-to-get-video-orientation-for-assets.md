# How to get video orientation for assets


Video orientation data is not readily available in Google Ads API but can be
fetched with [YouTube Data API](https://developers.google.com/youtube/v3/getting-started).\
In order to get this data for App Reporting Pack do the following:

- Enable [YouTube Data API](https://console.cloud.google.com/apis/library/youtube.googleapis.com)
* [API key](https://support.google.com/googleapi/answer/6158862?hl=en) to access to YouTube Data API.
  - Once you created API key export it as an environmental variable

    ```
    export GOOGLE_API_KEY=<YOUR_API_KEY_HERE>
    ```
