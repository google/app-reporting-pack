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

from __future__ import annotations

import argparse
import os
from dataclasses import asdict, dataclass

import google
import yaml
from gaarf.api_clients import GoogleAdsApiClient
from gaarf.bq_executor import BigQueryExecutor
from gaarf.cli.utils import (
  ConfigBuilder,
  init_logging,
)
from gaarf.io.reader import FileReader
from gaarf.io.writer import WriterFactory
from gaarf.query_executor import AdsReportFetcher
from gaarf.report import GaarfReport
from googleapiclient.discovery import build
from smart_open import open
from src import queries


@dataclass
class VideoOrientationConfig:
  mode: str
  params: dict


@dataclass
class CustomVideoOrientationRegexp:
  width_expression: str
  height_expression: str


@dataclass
class VideoOrientationRegexp:
  element_delimiter: str | None = None
  orientation_position: int | None = None
  orientation_delimiter: str | None = None


def update_config(
  path: str,
  mode: str,
  youtube_config_path: str = None,
  video_orientation_regexp: VideoOrientationRegexp
  | CustomVideoOrientationRegexp
  | None = None,
):
  if os.path.exists(path):
    with open(path, 'r', encoding='utf-8') as f:
      config = yaml.safe_load(f)
  else:
    config = {}
  scripts_config = {'scripts': {'video_orientation': {'mode': mode}}}
  if video_orientation_regexp:
    scripts_config['scripts']['video_orientation'].update(
      asdict(video_orientation_regexp)
    )
  if youtube_config_path:
    scripts_config['scripts']['video_orientation'].update(
      {'youtube_config_path': youtube_config_path}
    )
  config.update(scripts_config)
  with open(path, 'w', encoding='utf-8') as f:
    yaml.dump(
      config, f, default_flow_style=False, sort_keys=False, encoding='utf-8'
    )


class YouTubeDataConnector:
  def __init__(self, credentials, api_version: str = 'v3') -> None:
    self.service = build('youtube', api_version, credentials=credentials)

  def get_response(self, elements: str) -> list[tuple[str, float | None]]:
    videos = []
    video_orientations = []
    for i, element in enumerate(elements):
      videos.append(element)
      if i % 50 == 0:
        video_orientations.extend(
          self._parse_video_orientation(','.join(videos))
        )
        videos.clear()
    if videos:
      video_orientations.extend(self._parse_video_orientation(','.join(videos)))
    return GaarfReport(
      results=video_orientations, column_names=['video_id', 'video_orientation']
    )

  def _parse_video_orientation(
    self, videos: str
  ) -> list[tuple[str, str | None]]:
    video_orientations = []
    response = (
      self.service.videos().list(part='fileDetails', id=videos).execute()
    )
    aspect_ratio = None
    if items := response.get('items'):
      for item in items:
        youtube_video_id = item.get('id')
        if file_details := item.get('fileDetails'):
          if video_streams := file_details.get('videoStreams'):
            aspect_ratio = video_streams[0].get('aspectRatio')
        video_orientations.append(
          (youtube_video_id, self._convert_aspect_ratio(aspect_ratio))
        )
    return video_orientations

  def _convert_aspect_ratio(self, aspect_ratio: float | None) -> str:
    if not aspect_ratio:
      return 'Unknown'
    if aspect_ratio > 1:
      return 'Landscape'
    if aspect_ratio < 1:
      return 'Portrait'
    if aspect_ratio == 1:
      return 'Square'
    return 'Unknown'


def generate_placeholders(bq_executor, config) -> None:
  placeholder_query = """
            CREATE OR REPLACE TABLE `{bq_dataset}.video_orientation` AS
            SELECT DISTINCT
                video_id,
                "Unknown" AS video_orientation
            FROM `{bq_dataset}.mediafile`
            WHERE type = "VIDEO"
        """
  bq_executor.execute('video_orientation', placeholder_query, config.params)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-m', '--mode', dest='mode', default='placeholders')
  parser.add_argument('-c', '--config', dest='gaarf_config', default=None)
  parser.add_argument('--output', dest='save', default='bq')
  parser.add_argument('--ads-config', dest='ads_config')
  parser.add_argument('--account', dest='customer_id')
  parser.add_argument('--api-version', dest='api_version', default='12')
  parser.add_argument('--log', '--loglevel', dest='loglevel', default='info')
  parser.add_argument('--logger', dest='logger', default='local')
  parser.add_argument(
    '--customer-ids-query', dest='customer_ids_query', default=None
  )
  parser.add_argument(
    '--customer-ids-query-file', dest='customer_ids_query_file', default=None
  )

  parser.add_argument('--save-config', dest='save_config', action='store_true')
  parser.add_argument(
    '--config-destination', dest='save_config_dest', default='config.yaml'
  )

  parser.add_argument('--project', dest='project', default=None)
  parser.add_argument(
    '--dataset-location', dest='dataset_location', default=None
  )
  parser.add_argument(
    '--element-delimiter', dest='element_delimiter', default=None
  )
  parser.add_argument(
    '--orientation-position', dest='orientation_position', default=None
  )
  parser.add_argument(
    '--orientation-delimiter', dest='orientation_delimiter', default=None
  )
  parser.add_argument(
    '--youtube-config-path', dest='youtube_config_path', default=None
  )
  parser.add_argument(
    '--width-expression', dest='width_expression', default=None
  )
  parser.add_argument(
    '--height-expression', dest='height_expression', default=None
  )
  parser.add_argument('--dry-run', dest='dry_run', action='store_true')
  parser.set_defaults(save_config=False)
  parser.set_defaults(dry_run=False)
  args = parser.parse_known_args()

  logger = init_logging(
    loglevel=args[0].loglevel.upper(), logger_type=args[0].logger
  )

  mode = args[0].mode
  if args[0].gaarf_config:
    with open(args[0].gaarf_config, 'r', encoding='utf-8') as f:
      gaarf_config = yaml.safe_load(f)
      if scripts := gaarf_config.get('scripts'):
        if video_orientation_config := scripts.get('video_orientation'):
          mode = video_orientation_config.get('mode')
          element_delimiter = video_orientation_config.get('element_delimiter')
          orientation_position = video_orientation_config.get(
            'orientation_position'
          )
          orientation_delimiter = video_orientation_config.get(
            'orientation_delimiter'
          )
          youtube_config_path = video_orientation_config.get(
            'youtube_config_path'
          )
          width_expression = video_orientation_config.get('width_expression')
          height_expression = video_orientation_config.get('height_expression')
      else:
        element_delimiter = None
        orientation_position = None
        orientation_delimiter = None
        width_expression = None
        height_expression = None

  save_config = args[0].save_config
  dry_run = args[0].dry_run
  if dry_run:
    save_config = True
  bq_config = ConfigBuilder('gaarf-bq').build(
    vars(args[0]), args[1]
  )
  bq_executor = BigQueryExecutor(bq_config.project)
  if mode == 'youtube':
    if save_config:
      update_config(
        path=args[0].gaarf_config,
        mode=mode,
        youtube_config_path=args[0].youtube_config_path,
      )

    if dry_run:
      exit()
    logger.info('Getting video orientation from YouTube')
    with open(
      args[0].youtube_config_path or youtube_config_path, 'r', encoding='utf-8'
    ) as f:
      youtube_config = yaml.safe_load(f)
    try:
      parsed_videos = bq_executor.execute(
        script_name='existing_videos',
        query_text="""
                SELECT DISTINCT
                    video_id,
                    video_orientation
                FROM {bq_dataset}.video_orientation
                """,
        params=bq_config.params,
      )
    except Exception:
      parsed_videos = set()
    with open(args[0].ads_config, 'r', encoding='utf-8') as f:
      google_ads_config_dict = yaml.safe_load(f)
    config = GaarfConfigBuilder(args).build()
    google_ads_client = GoogleAdsApiClient(
      config_dict=google_ads_config_dict, version=f'v{config.api_version}'
    )
    report_fetcher = AdsReportFetcher(google_ads_client)
    customer_ids = report_fetcher.expand_mcc(
      config.account, config.customer_ids_query
    )
    videos = report_fetcher.fetch(queries.Videos(), customer_ids).to_list()
    if parsed_videos:
      videos = list(set(videos).difference(set(parsed_videos['video_id'])))
    else:
      videos = list(set(videos))
    if videos:
      credentials = google.oauth2.credentials.Credentials(
        None,
        refresh_token=youtube_config.get('refresh_token'),
        token_uri='https://oauth2.googleapis.com/token',
        client_id=youtube_config.get('client_id'),
        client_secret=youtube_config.get('client_secret'),
      )
      youtube_data_connector = YouTubeDataConnector(credentials)
      try:
        video_orientations = youtube_data_connector.get_response(videos)
        writer_client = WriterFactory().create_writer(
          config.output, **config.writer_params
        )
        writer_client.write(video_orientations, 'video_orientation')
      except Exception:
        logger.warning(
          'Unable to access YouTube Data API, generating placeholders'
        )
        generate_placeholders(bq_executor, bq_config)
        mode = 'placeholders'
    else:
      logger.info('No new videos to parse')

  elif mode == 'regex':
    logger.info('Parsing video orientation from asset name based on regexp')
    script_path = os.path.dirname(__file__)
    video_orientation_regexp = VideoOrientationRegexp(
      element_delimiter=args[0].element_delimiter,
      orientation_position=args[0].orientation_position,
      orientation_delimiter=args[0].orientation_delimiter,
    )
    if save_config:
      update_config(
        path=args[0].gaarf_config,
        mode=mode,
        video_orientation_regexp=video_orientation_regexp,
      )
    if dry_run:
      exit()
    bq_executor.execute(
      'video_orientation',
      FileReader().read(os.path.join(script_path, 'src/video_orientation.sql')),
      {
        'macro': {
          'bq_dataset': bq_config.params.get('macro').get('bq_dataset'),
          'element_delimiter': args[0].element_delimiter or element_delimiter,
          'orientation_position': args[0].orientation_position
          or orientation_position,
          'orientation_delimiter': args[0].orientation_delimiter
          or orientation_delimiter,
        }
      },
    )
  elif mode == 'custom_regex':
    logger.info(
      'Parsing video orientation from asset name based on custom regexp'
    )
    script_path = os.path.dirname(__file__)
    video_orientation_regexp = CustomVideoOrientationRegexp(
      width_expression=args[0].width_expression,
      height_expression=args[0].height_expression,
    )
    if save_config:
      update_config(
        path=args[0].gaarf_config,
        mode=mode,
        video_orientation_regexp=video_orientation_regexp,
      )
    if dry_run:
      exit()
    bq_executor.execute(
      'video_orientation',
      FileReader().read(
        os.path.join(script_path, 'src/video_orientation_custom_regexp.sql')
      ),
      {
        'macro': {
          'bq_dataset': bq_config.params.get('macro').get('bq_dataset'),
          'width_expression': args[0].width_expression or width_expression,
          'height_expression': args[0].height_expression or height_expression,
        }
      },
    )
  else:
    logger.info('Generating placeholders for video orientation')
    mode = 'placeholders'
    video_orientation_regexp = None
    if save_config:
      update_config(path=args[0].gaarf_config, mode=mode)
    if dry_run:
      exit()
    generate_placeholders(bq_executor, bq_config)


if __name__ == '__main__':
  main()
