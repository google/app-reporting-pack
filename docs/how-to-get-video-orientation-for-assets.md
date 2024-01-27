# How to get video orientation for assets

Video dimension (width x height) cannot be fetched from Google Ads API.

There are several ways to get this data into App Reporting Pack:
1. [Get video orientation from asset names](#get-video-orientation-from-asset-names)
    1. [Use custom regexp for complex cases](#use-custom-regexp-for-complex-cases)
1. [Get video orientation from YouTube Data API](#get-video-orientation-from-youtube-data-api)
1. [Use placeholders](#use-placeholders)

## Get video orientation from asset names

If you have a consistent naming for your videos you extract width and height
from asset name.

Let's consider the following example:

1. You have a video titled  `my_video_1000x1000_concept_name.mp4`
2. Video dimension (width x height) is `1000x1000`
3. In order to get this values we need:
    1. Split video name by `_` delimiter
    2. Identify the position of (width x height) (if starting from 0 it's position 2)
    3. Split video dimension by `x` delimiter

App Reporting Pack interactive installer will ask all these questions when performing
the solution deployment.

Alternatively you can add the following section into your `app_reporting_pack.yaml` config file:

```
scripts:
  video_orientation:
    mode: regex
    element_delimiter: '_'
    orientation_position: '2'
    orientation_delimiter: 'x'
```

### Use custom regexp for complex cases

If the rules for getting video orientation are complex and cannot be expressed
with a single position approach explained above you can use `custom_regex` marker.\
To activate custom_regex parsing add the following section into your `app_reporting_pack.yaml` config file:

```
scripts:
  video_orientation:
    mode: custom_regex
    width_expression: 'YOUR_WIDTH_REGEXP_HERE'
    height_expression: 'YOUR_HEIGHT_REGEXP_HERE'
```

## Get video orientation from YouTube Data API

If getting data from video names is impossible you can always fetch video dimension
directly from YouTube Data API.
> Fetching data from YouTube API requires authorization - you can follow steps at
> [Setup YouTube Data API access](#setup-youtube-data-api-access).

App Reporting Pack interactive installer will ask to provide full path to `youtube_config.yaml` file

Alternatively you can add the following section into your `app_reporting_pack.yaml` config file:

```
scripts:
  video_orientation:
    mode: youtube
    youtube_config_path: youtube_config.yaml
```

### Setup YouTube Data API access

You can fetch video orientation from YouTube Data API.
This data only available to that video's owner.

In order to get access to video orientation details you need to be properly authorized.

1. Create file `youtube_config.yaml` with the following elements.

```
client_id:
client_secret:
refresh_token:
```

2. Setup Google Cloud project and OAuth client id

Create an OAuth credentials (for the first time you'll need to create a concent screen either).
Please note you need to generate OAuth2 credentials for **desktop application**.
Copy `client_id` and `client_secret` to `youtube_config.yaml`.


First you need to download [oauth2l](https://github.com/google/oauth2l) tool ("oauth tool").
For Windows and Linux please download [pre-compiled binaries](https://github.com/google/oauth2l#pre-compiled-binaries),
for MacOS you can install via Homebrew: `brew install oauth2l`.

As soon as you generated OAuth2 credentials:
* Click the download icon next to the credentials that you just created and save file to your computer
* Copy the file name under which you saved secrets file -
`~/client_secret_XXX.apps.googleusercontent.com.json` where XXX will be values specific to your project
(or just save it under `client_secret.json` name for simplicity)
* Run desktop authentication with downloaded credentials file using oauth2l in the same folder where you put the downloaded secret file (assuming its name is client_secret.json):
```
oauth2l fetch --credentials ./client_secret.json  --scope youtube --output_format refresh_token
```
* Copy a refresh token from the output and add to `youtube_config.yaml`


## Use placeholders

If it's not possible to get video dimensions from both video names or YouTube Data API
App Reporting Pack will generate the placeholders - each video will have `Unknown`
video orientation.

You can explicitly add the following section into your `app_reporting_pack.yaml` config file to specify that you're working with placeholders:

```
scripts:
  video_orientation:
    mode: placeholders
```
