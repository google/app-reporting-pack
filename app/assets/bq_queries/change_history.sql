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

-- Contains daily changes (bids, budgets, assets, etc) on campaign level.
CREATE OR REPLACE TABLE `{target_dataset}.change_history`
AS (
  WITH
    AssetStructureDailyArrays AS (
      SELECT
        day,
        ad_group_id,
        SPLIT(install_headlines, '|') AS install_headlines,
        SPLIT(install_descriptions, '|') AS install_descriptions,
        SPLIT(engagement_headlines, '|') AS engagement_headlines,
        SPLIT(engagement_descriptions, '|') AS engagement_descriptions,
        SPLIT(pre_registration_headlines, '|') AS pre_registration_headlines,
        SPLIT(pre_registration_descriptions, '|')
          AS pre_registration_descriptions,
        SPLIT(install_images, '|') AS install_images,
        SPLIT(install_videos, '|') AS install_videos,
        SPLIT(engagement_images, '|') AS engagement_images,
        SPLIT(engagement_videos, '|') AS engagement_videos,
        SPLIT(pre_registration_images, '|') AS pre_registration_images,
        SPLIT(pre_registration_videos, '|') AS pre_registration_videos,
        SPLIT(install_media_bundles, '|') AS install_media_bundles
      FROM `{bq_dataset}.asset_structure_snapshot_*`
    ),
    AssetChangesRaw AS (
      SELECT
        day,
        ad_group_id,
        -- installs
        CASE
          WHEN
            install_headlines IS NULL OR LAG(install_headlines)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              install_headlines,
              LAG(install_headlines)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS install_headlines_changes,
        CASE
          WHEN
            install_descriptions IS NULL OR LAG(install_descriptions)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              install_descriptions,
              LAG(install_descriptions)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS install_descriptions_changes,
        CASE
          WHEN
            install_videos IS NULL OR LAG(install_videos)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              install_videos,
              LAG(install_videos) OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS install_videos_changes,
        CASE
          WHEN
            install_images IS NULL OR LAG(install_images)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              install_images,
              LAG(install_images) OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS install_images_changes,
        CASE
          WHEN
            install_media_bundles IS NULL OR LAG(install_media_bundles)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              install_media_bundles,
              LAG(install_media_bundles)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS install_media_bundles_changes,
        -- engagements
        CASE
          WHEN
            engagement_headlines IS NULL OR LAG(engagement_headlines)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              engagement_headlines,
              LAG(engagement_headlines)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS engagement_headlines_changes,
        CASE
          WHEN
            engagement_descriptions IS NULL OR LAG(engagement_descriptions)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              engagement_descriptions,
              LAG(engagement_descriptions)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS engagement_descriptions_changes,
        CASE
          WHEN
            engagement_videos IS NULL OR LAG(engagement_videos)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              engagement_videos,
              LAG(engagement_videos)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS engagement_videos_changes,
        CASE
          WHEN
            engagement_images IS NULL OR LAG(engagement_images)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              engagement_images,
              LAG(engagement_images)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS engagement_images_changes,
        CASE
          WHEN
            pre_registration_headlines IS NULL OR LAG(pre_registration_headlines)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              pre_registration_headlines,
              LAG(pre_registration_headlines)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS pre_registration_headlines_changes,
        CASE
          WHEN
            pre_registration_descriptions IS NULL OR LAG(pre_registration_descriptions)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              pre_registration_descriptions,
              LAG(pre_registration_descriptions)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS pre_registration_descriptions_changes,
        -- pre_registrations
        CASE
          WHEN
            pre_registration_videos IS NULL OR LAG(pre_registration_videos)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              pre_registration_videos,
              LAG(pre_registration_videos)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS pre_registration_videos_changes,
        CASE
          WHEN
            pre_registration_images IS NULL OR LAG(pre_registration_images)
              OVER (PARTITION BY ad_group_id ORDER BY day)
            IS NULL
            THEN 0
          ELSE
            `{bq_dataset}.equalsArr`(
              pre_registration_images,
              LAG(pre_registration_images)
                OVER (PARTITION BY ad_group_id ORDER BY day))
          END AS pre_registration_images_changes
      FROM AssetStructureDailyArrays
    ),
    AssetChanges AS (
      SELECT
        A.day,
        M.campaign_id,
        SUM(A.install_headlines_changes)
          + SUM(A.engagement_headlines_changes)
          + SUM(A.pre_registration_headlines_changes) AS headline_changes,
        SUM(A.install_descriptions_changes)
          + SUM(A.engagement_descriptions_changes)
          + SUM(A.pre_registration_descriptions_changes) AS description_changes,
        SUM(A.install_images_changes)
          + SUM(A.engagement_images_changes)
          + SUM(A.pre_registration_images_changes) AS image_changes,
        SUM(A.install_videos_changes)
          + SUM(A.engagement_videos_changes)
          + SUM(A.pre_registration_videos_changes) AS video_changes,
        SUM(A.install_media_bundles_changes) AS html5_changes,
      FROM AssetChangesRaw AS A
      LEFT JOIN `{bq_dataset}.account_campaign_ad_group_mapping` AS M
        USING (ad_group_id)
      GROUP BY 1, 2
    )
  SELECT
    CP.*,
    IFNULL(AC.headline_changes + AC.description_changes, 0)
      AS text_changes,
    IFNULL(AC.image_changes, 0) AS image_changes,
    IFNULL(AC.html5_changes, 0) AS html5_changes,
    IFNULL(AC.video_changes, 0) AS video_changes
  FROM `{target_dataset}.change_history` AS CP
  LEFT JOIN AssetChanges AS AC
    USING (campaign_id, day)
);
