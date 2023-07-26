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

-- Get video aspect ratio.
CREATE TEMP FUNCTION GetVideoOrientation (video_orientation_expr STRING, delimiter STRING)
RETURNS FLOAT64
AS (
    SAFE_DIVIDE(
        SAFE_CAST(SPLIT(video_orientation_expr, delimiter)[SAFE_OFFSET(0)] AS INT64),
        SAFE_CAST(SPLIT(video_orientation_expr, delimiter)[SAFE_OFFSET(1)] AS INT64)
    )
);

-- Convert video aspect ratio to orientation.
CREATE TEMP FUNCTION CalculateVideoOrientation(video_orientation_expr STRING, delimiter STRING)
RETURNS STRING
AS (
    CASE
        WHEN GetVideoOrientation(video_orientation_expr, delimiter) > 1 THEN "Landscape"
        WHEN GetVideoOrientation(video_orientation_expr, delimiter) = 1 THEN "Square"
        WHEN GetVideoOrientation(video_orientation_expr, delimiter) < 1 THEN "Portrait"
        ELSE "Unknown"
        END
);

CREATE OR REPLACE TABLE `{bq_dataset}.video_orientation` AS
WITH Mapping AS (
    SELECT DISTINCT
        video_id,
        name
    FROM `{bq_dataset}.mediafile`
    WHERE type = "VIDEO"
)
SELECT
video_id,
CalculateVideoOrientation(
    SPLIT(name, "{element_delimiter}")[SAFE_OFFSET({orientation_position})],
        "{orientation_delimiter}"
) AS video_orientation
FROM Mapping;
