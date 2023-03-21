# Setup YouTube Data API to fetch video orientation

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
