-- Copyright 2023 Google LLC
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
    WITH AssetStructureDailyArrays AS (
       SELECT
            day,
            ad_group_id,
            SPLIT(install_headlines, "|") AS install_headlines,
            SPLIT(install_descriptions, "|") AS install_descriptions,
            SPLIT(engagement_headlines, "|") AS engagement_headlines,
            SPLIT(engagement_descriptions, "|") AS engagement_descriptions,
            SPLIT(pre_registration_headlines, "|") AS pre_registration_headlines,
            SPLIT(pre_registration_descriptions, "|") AS pre_registration_descriptions,
            SPLIT(install_images, "|") AS install_images,
            SPLIT(install_videos, "|") AS install_videos,
            SPLIT(engagement_images, "|") AS engagement_images,
            SPLIT(engagement_videos, "|") AS engagement_videos,
            SPLIT(pre_registration_images, "|") AS pre_registration_images,
            SPLIT(pre_registration_videos, "|") AS pre_registration_videos,
            SPLIT(install_media_bundles, "|") AS install_media_bundles
        FROM `{bq_dataset}.asset_structure_snapshot_*`
    ),
    AssetChangesRaw AS (
        SELECT
        day,
        ad_group_id,
        -- installs
        CASE
            WHEN LAG(install_headlines) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(install_headlines, LAG(install_headlines) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_headlines_changes,
        CASE
            WHEN LAG(install_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(install_descriptions, LAG(install_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_descriptions_changes,

        CASE
            WHEN LAG(install_videos) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(install_videos, LAG(install_videos) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_videos_changes,
        CASE
            WHEN LAG(install_images) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(install_images, LAG(install_images) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_images_changes,
        CASE
            WHEN LAG(install_media_bundles) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(install_media_bundles, LAG(install_media_bundles) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS install_media_bundles_changes,
        -- engagements
        CASE
            WHEN LAG(engagement_headlines) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(engagement_headlines, LAG(engagement_headlines) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS engagement_headlines_changes,
        CASE
            WHEN LAG(engagement_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(engagement_descriptions, LAG(engagement_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS engagement_descriptions_changes,
        CASE
            WHEN LAG(engagement_videos) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(engagement_videos, LAG(engagement_videos) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS engagement_videos_changes,
        CASE
            WHEN LAG(engagement_images) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(engagement_images, LAG(engagement_images) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS engagement_images_changes,
        CASE
            WHEN LAG(pre_registration_headlines) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(pre_registration_headlines, LAG(pre_registration_headlines) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS pre_registration_headlines_changes,
        CASE
            WHEN LAG(pre_registration_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(pre_registration_descriptions, LAG(pre_registration_descriptions) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS pre_registration_descriptions_changes,
        -- pre_registrations
        CASE
            WHEN LAG(pre_registration_videos) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(pre_registration_videos, LAG(pre_registration_videos) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS pre_registration_videos_changes,
        CASE
            WHEN LAG(pre_registration_images) OVER (PARTITION BY ad_group_id ORDER BY day) IS NULL THEN 0
            ELSE `{bq_dataset}.equalsArr`(pre_registration_images, LAG(pre_registration_images) OVER (PARTITION BY ad_group_id ORDER BY day))
            END AS pre_registration_images_changes
        FROM AssetStructureDailyArrays
    ),
    AssetChanges AS (
        SELECT
            day,
            campaign_id,
            SUM(install_headlines_changes) + SUM(engagement_headlines_changes) + SUM(pre_registration_headlines_changes) AS headline_changes,
            SUM(install_descriptions_changes) + SUM(engagement_descriptions_changes) + SUM(pre_registration_descriptions_changes) AS description_changes,
            SUM(install_images_changes) + SUM(engagement_images_changes) + SUM(pre_registration_images_changes) AS image_changes,
            SUM(install_videos_changes) + SUM(engagement_videos_changes) + SUM(pre_registration_videos_changes) AS video_changes,
            SUM(install_media_bundles_changes) AS html5_changes,
        FROM AssetChangesRaw
        LEFT JOIN `{bq_dataset}.account_campaign_ad_group_mapping`
            USING(ad_group_id)
        GROUP BY 1, 2
    )
SELECT
    CP.*,
    AssetChanges.headline_changes + AssetChanges.description_changes AS text_changes,
    AssetChanges.image_changes AS image_changes,
    AssetChanges.html5_changes AS html5_changes,
    AssetChanges.video_changes AS video_changes
FROM `{target_dataset}.change_history` AS CP
LEFT JOIN AssetChanges USING(campaign_id, day)
);
