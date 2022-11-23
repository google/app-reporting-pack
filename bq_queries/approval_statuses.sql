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

-- Contains dynamics by approval status.
CREATE OR REPLACE TABLE {target_dataset}.approval_statuses
AS (
WITH
    AdGroupDisapprovals AS (
        SELECT
            day,
            ad_group_id,
            -1 AS asset_id,
            "Ad" AS field_type,
            approval_status,
            review_status,
            policy_topics,
            policy_topic_type,
            evidences
        FROM `{bq_dataset}.ad_group_approval_statuses_*`
    ),
    AssetDisapprovals AS (
        SELECT
            day,
            ad_group_id,
            field_type,
            asset_id,
            approval_status,
            review_status,
            policy_topics,
            policy_topic_type,
            evidences,
        FROM `{bq_dataset}.asset_approval_statuses_*`
    ),
    CombinedDisapprovals AS (
        SELECT
            day,
            ad_group_id,
            asset_id,
            field_type,
            approval_status,
            review_status,
            policy_topics,
            policy_topic_type,
            evidences
        FROM AdGroupDisapprovals
        UNION ALL
        SELECT
            day,
            ad_group_id,
            asset_id,
            field_type,
            approval_status,
            review_status,
            policy_topics,
            policy_topic_type,
            evidences
        FROM AssetDisapprovals
    )
SELECT
    day,
    M.account_id,
    M.account_name,
    M.currency,
    M.campaign_id,
    M.campaign_name,
    M.campaign_status,
    ACS.campaign_sub_type,
    ACS.app_id,
    ACS.app_store,
    ACS.bidding_strategy,
    ACS.target_conversions,
    ACS.start_date AS start_date,
    G.geos,
    G.languages,
    M.ad_group_id,
    M.ad_group_name,
    M.ad_group_status,
    IF(Disapprovals.asset_id = -1, field_type, `{bq_dataset}.ConvertAssetFieldType`(field_type)) AS disapproval_level,
    Disapprovals.asset_id,
    CASE Assets.type
        WHEN "TEXT" THEN Assets.text WHEN "IMAGE" THEN Assets.asset_name
        WHEN "MEDIA_BUNDLE" THEN Assets.asset_name
        WHEN "YOUTUBE_VIDEO" THEN Assets.youtube_video_title
        ELSE NULL
        END AS asset,
    CASE Assets.type
        WHEN "TEXT" THEN ""
        WHEN "IMAGE" THEN Assets.url
        WHEN "MEDIA_BUNDLE" THEN Assets.url
        WHEN "YOUTUBE_VIDEO" THEN CONCAT("https://www.youtube.com/watch?v=", Assets.youtube_video_id)
        ELSE NULL
        END AS asset_link,
    Disapprovals.approval_status,
    Disapprovals.review_status,
    Disapprovals.policy_topics,
    Disapprovals.policy_topic_type,
    Disapprovals.evidences
FROM CombinedDisapprovals AS Disapprovals
LEFT JOIN {bq_dataset}.account_campaign_ad_group_mapping AS M
  ON Disapprovals.ad_group_id = M.ad_group_id
LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
  ON M.campaign_id = ACS.campaign_id
LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
  ON M.campaign_id =  G.campaign_id
LEFT JOIN {bq_dataset}.asset_mapping AS Assets
  ON Disapprovals.asset_id = Assets.id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27);
