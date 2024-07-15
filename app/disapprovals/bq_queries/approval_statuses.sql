-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Contains dynamics by approval status.
CREATE OR REPLACE TABLE {target_dataset}.approval_statuses
AS (
  WITH
    AdGroupDisapprovals AS (
      SELECT
        day,
        ad_group_id,
        -1 AS asset_id,
        'Ad' AS field_type,
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
    ),
    MappingTable AS (
      SELECT
        M.ad_group_id,
        ANY_VALUE(M.ad_group_name) AS ad_group_name,
        ANY_VALUE(M.ad_group_status) AS ad_group_status,
        ANY_VALUE(M.campaign_id) AS campaign_id,
        ANY_VALUE(M.campaign_name) AS campaign_name,
        ANY_VALUE(M.campaign_status) AS campaign_status,
        ANY_VALUE(M.account_id) AS account_id,
        ANY_VALUE(M.account_name) AS account_name,
        ANY_VALUE(O.ocid) AS ocid,
        ANY_VALUE(M.currency) AS currency
      FROM `{bq_dataset}.account_campaign_ad_group_mapping` AS M
      LEFT JOIN `{bq_dataset}.ocid_mapping` AS O
        USING (account_id)
      GROUP BY 1
    )
  SELECT
    D.day,
    M.account_id,
    M.account_name,
    M.ocid,
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
    IF(
      D.asset_id = -1,
      field_type,
      `{bq_dataset}.ConvertAssetFieldType`(A.field_type)) AS disapproval_level,
    D.asset_id,
    CASE A.type
      WHEN 'TEXT' THEN A.text
      WHEN 'IMAGE' THEN A.asset_name
      WHEN 'MEDIA_BUNDLE' THEN A.asset_name
      WHEN 'YOUTUBE_VIDEO' THEN A.youtube_video_title
      ELSE NULL
      END AS asset,
    CASE A.type
      WHEN 'TEXT' THEN ''
      WHEN 'IMAGE' THEN A.url
      WHEN 'MEDIA_BUNDLE' THEN A.url
      WHEN 'YOUTUBE_VIDEO'
        THEN CONCAT('https://www.youtube.com/watch?v=', A.youtube_video_id)
      ELSE NULL
      END AS asset_link,
    D.approval_status,
    D.review_status,
    D.policy_topics,
    D.policy_topic_type,
    D.evidences
  FROM CombinedDisapprovals AS D
  LEFT JOIN MappingTable AS M
    ON D.ad_group_id = M.ad_group_id
  LEFT JOIN `{bq_dataset}.AppCampaignSettingsView` AS ACS
    ON M.campaign_id = ACS.campaign_id
  LEFT JOIN `{bq_dataset}.GeoLanguageView` AS G
    ON M.campaign_id = G.campaign_id
  LEFT JOIN {bq_dataset}.asset_mapping AS A
    ON D.asset_id = A.id
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
    22, 23, 24, 25, 26, 27, 28
);
