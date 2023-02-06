# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CREATE OR REPLACE VIEW {legacy_dataset}.uacassets AS
SELECT
    Day,
    account_name AS AccountName,
    account_id,
    "" AS CID,
    campaign_sub_type AS CampaignSubType,
    CASE app_store
        WHEN 'GOOGLE_APP_STORE' THEN 'Google Play'
        WHEN 'APPLE_APP_STORE' THEN 'App Store'
        ELSE 'Other'
    END AS Store,
    app_id AS AppId,
    campaign_name AS CampaignName,
    campaign_id AS CampaignID,
    CASE campaign_status
        WHEN "ENABLED" THEN "Active"
        WHEN "PAUSED" THEN "Paused"
        WHEN "REMOVED" THEN "Deleted"
    END AS CampaignStatus,
    campaign_cost AS CampaignCost,
    ad_group_id AS AdGroupId,
    ad_group_name AS AdGroup,
    CASE ad_group_status
        WHEN "ENABLED" THEN "Active"
        WHEN "PAUSED" THEN "Paused"
        WHEN "REMOVED" THEN "Deleted"
    END AS AdGroupStatus,
    CASE
        WHEN bidding_strategy = "Installs" THEN "UAC Installs"
        WHEN bidding_strategy = "Installs Advanced" THEN "UAC Installs Advanced"
        WHEN bidding_strategy = "Actions" THEN "UAC Actions"
        WHEN bidding_strategy = "Maximize Conversions" THEN "UAC Maximize Conversions"
        WHEN bidding_strategy = "Target ROAS" THEN "UAC ROAS"
        WHEN bidding_strategy = "Preregistrations" THEN "UAC Pre-Registrations"
        ELSE "Unknown"
    END AS UACType,
    target_conversions AS TargetConversions,
    Currency,
    geos AS country_code,
    languages AS Language,
    REGEXP_CONTAINS(languages, r"en|All") AS English,
    CASE asset_type
        WHEN "TEXT" THEN "Text"
        WHEN "YOUTUBE_VIDEO" THEN "Video"
        WHEN "IMAGE" THEN "Image"
        WHEN "MEDIA_BUNDLE" THEN "Html5"
    END AS AssetType,
    asset AS AdAsset,
    asset_id AS FeedDataOriginal,
    asset AS FeedData,
    performance_label AS PerformanceGrouping,
    0 AS mediaid,
    asset_link AS sourceUrl,
    0 AS mediafilesize,
    "" AS LinkToCampaign,
    IF(asset_type = "YOUTUBE_VIDEO", asset_orientation, "")  AS Link,
    asset_orientation AS ImageSize,
    asset_orientation AS VideoOrientation,
    CASE
        WHEN ROUND(video_aspect_ratio, 2) = 1.78 THEN "16:9"
        WHEN ROUND(video_aspect_ratio, 2) = 1.50 THEN "3:2"
        WHEN ROUND(video_aspect_ratio, 2) = 1.33 THEN "4:3"
        WHEN ROUND(video_aspect_ratio, 2) = 1.25 THEN "5:4"
        WHEN ROUND(video_aspect_ratio, 2) = 1.00 THEN "1:1"
        WHEN ROUND(video_aspect_ratio, 2) = 0.80 THEN "4:5"
        WHEN ROUND(video_aspect_ratio, 2) = 0.75 THEN "3:4"
        WHEN ROUND(video_aspect_ratio, 2) = 0.67 THEN "2:3"
        WHEN ROUND(video_aspect_ratio, 2) = 0.56 THEN "9:16"
        WHEN video_aspect_ratio IS NULL THEN "Unknown"
        ELSE "Other"
        END AS video_aspect_ratio,
    CASE
        WHEN ROUND(video_duration) > 31 THEN "31+"
        WHEN ROUND(video_duration) > 25 THEN "26-30"
        WHEN ROUND(video_duration) > 20 THEN "21-25"
        WHEN ROUND(video_duration) > 15 THEN "16-20"
        WHEN ROUND(video_duration) > 10 THEN "11-15"
        WHEN ROUND(video_duration) > 6 THEN "07-10"
        WHEN ROUND(video_duration) > 0 THEN "06-"
        ELSE "Unknown"
        END AS video_length_bucket,
    "" AS RecommendedImage,
    "" AS ImageSizeType,
    CASE
        -- images and html5
        WHEN asset_orientation IN ("1200x628", "1200x627", "600x314", "2400x1256") THEN "1200x628"
        WHEN asset_orientation IN ("320x480", "640x960") THEN "320x480"
        WHEN asset_orientation IN ("480x320", "960x640") THEN "480x320"
        WHEN asset_orientation IN ("300x250", "600x500") THEN "300x250"
        WHEN asset_orientation IN ("300x50", "600x100") THEN "300x50"
        WHEN asset_orientation IN ("320x50", "640x100") THEN "320x50"
        WHEN asset_orientation IN ("320x100", "640x200") THEN "320x100"
        WHEN asset_orientation IN ("728x90", "1456x180") THEN "728x90"
        WHEN asset_orientation IN ("250x250", "500x500") THEN "250x250"
        WHEN asset_orientation IN ("240x400", "480x800") THEN "240x400"
        -- headlines
        WHEN field_type = "HEADLINE" AND LENGTH(asset) <= 20 THEN "0-20 symbols"
        WHEN field_type = "HEADLINE" AND LENGTH(asset) > 20 THEN "20+ symbols"
        -- description
        WHEN field_type = "DESCRIPTION" AND LENGTH(asset) <= 30 THEN "0-23 symbols"
        WHEN field_type = "DESCRIPTION" AND LENGTH(asset) <= 60 THEN "31-60 symbols"
        WHEN field_type = "DESCRIPTION" AND LENGTH(asset) > 60 THEN "60+ symbols"
        -- video
        ELSE asset_orientation
        END AS RefinedImageSize,
    "" AS CreationTime,
    CASE asset_status
        WHEN "ENABLED" THEN "Active"
        WHEN "PAUSED" THEN "Paused"
        WHEN "REMOVED" THEN "Deleted"
    END AS Status,
    Network,
    0 AS Device,
    firebase_bidding_status,
    clicks,
    impressions,
    cost,
    cost_non_install_campaigns AS CostNonInstalls,
    inapps + installs AS conversions,
    view_through_conversions,
    installs,
    installs_adjusted,
    inapps AS InApp,
    inapps_adjusted,
    0 AS non_click_interactions,
    0 AS engagements,
    conversions_value AS conversion_value,
    {% for day in cohort_days %}
            installs_{{day}}_day,
            inapps_{{day}}_day AS InApp_day{{day}},
            conversions_value_{{day}}_day AS conversion_value_day{{day}},
        {% endfor %}
FROM {target_dataset}.asset_performance;
