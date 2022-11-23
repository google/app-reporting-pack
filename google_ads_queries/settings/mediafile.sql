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

# Get meta information on each media element
SELECT
    media_file.id AS media_id,
    media_file.type AS type,
    media_file.name AS name,
    media_file.image.full_size_image_url AS url,
    media_file.source_url AS source_url,
    media_file.media_bundle.url AS bundle_url,
    media_file.video.youtube_video_id AS video_id,
    media_file.video.ad_duration_millis AS video_duration
FROM media_file
