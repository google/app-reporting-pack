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

-- Performs association between each asset_id and its corresponding meta
-- information (size for images and HTML5, actual text for headlines and
-- descriptions, etc.)

SELECT
    asset.id AS id,
    asset.name AS asset_name,
    asset.text_asset.text AS text,
    asset.image_asset.full_size.height_pixels AS height,
    asset.image_asset.full_size.width_pixels AS width,
    asset.image_asset.full_size.url AS url,
    asset.type AS type,
    asset.image_asset.mime_type AS mime_type,
    asset.youtube_video_asset.youtube_video_id AS youtube_video_id,
    asset.youtube_video_asset.youtube_video_title AS youtube_video_title
FROM asset
