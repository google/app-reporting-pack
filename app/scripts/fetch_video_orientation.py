# Copyright 2023 Google LLC
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

# pylint: disable=C0330, g-bad-import-order, g-multiple-import

"""Module for performing video orientation parsing."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

import garf_core
import garf_youtube_data_api
from gaarf.cli import utils as gaarf_utils
from garf_executors import bq_executor
from garf_io.writers import bigquery_writer


def get_video_orientations_from_youtube_data_api(
  videos: Sequence[str],
) -> garf_core.report.GarfReport:
  """Fetches video orientations based on YouTube Data API."""
  youtube_video_orientations_query = """
  SELECT
    id AS video_id,
    player.embedWidth AS width,
    player.embedHeight AS height
  FROM videos
  """

  youtube_api_fetcher = garf_youtube_data_api.YouTubeDataApiReportFetcher()
  video_orientations = youtube_api_fetcher.fetch(
    youtube_video_orientations_query,
    id=videos,
    maxWidth=500,
  )
  for row in video_orientations:
    row['aspect_ratio'] = round(int(row.width) / int(row.height), 2)
    if row['aspect_ratio'] > 1:
      row['video_orientation'] = 'Landscape'
    elif row['aspect_ratio'] < 1:
      row['video_orientation'] = 'Portrait'
    else:
      row['video_orientation'] = 'Square'
  return video_orientations


def generate_placeholders(videos: Sequence[str]) -> garf_core.report.GarfReport:
  """Creates 'Unknown' orientation for every video."""
  results = []
  for video in videos:
    results.append([video, 'Unknown'])
  return garf_core.report.GarfReport(
    results=results, column_names=['video_id', 'video_orientation']
  )


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--config', dest='gaarf_config', default=None)
  parser.add_argument('--log', '--loglevel', dest='loglevel', default='info')
  parser.add_argument('--logger', dest='logger', default='local')
  args, kwargs = parser.parse_known_args()

  gaarf_utils.init_logging(
    loglevel=args.loglevel.upper(), logger_type=args.logger
  )

  config = gaarf_utils.ConfigBuilder('gaarf').build(vars(args), kwargs)
  bq_project = config.writer_params.get('project')
  bq_dataset = config.writer_params.get('dataset')
  videos = (
    bq_executor.BigQueryExecutor(bq_project)
    .execute(
      script_name='video_orientations',
      query_text=f"""
      SELECT DISTINCT
        youtube_video_id
      FROM `{bq_dataset}.asset_mapping`
      WHERE type = 'YOUTUBE_VIDEO'
      """,
    )
    .youtube_video_id.to_list()
  )
  try:
    video_orientations = get_video_orientations_from_youtube_data_api(videos)
  except Exception as e:
    logging.error(
      'Failed to get data from YouTube Data API, generating placeholders'
    )
    logging.debug(e)
    video_orientations = generate_placeholders(videos)
  bigquery_writer.BigQueryWriter(
    project=bq_project,
    dataset=bq_dataset,
  ).write(
    video_orientations[['video_id', 'video_orientation']], 'video_orientation'
  )


if __name__ == '__main__':
  main()
