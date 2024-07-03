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
"""Module for performing video orientation parsing.

Video orientation for assets can be identified as one of the method:
* by calling YouTube Data API (requires owner permissions)
* by extracting from asset name
* by defining a placeholder
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import os
from typing import TypedDict

import gaarf
import google
import smart_open
import yaml
from gaarf import api_clients
from gaarf.cli import utils as gaarf_utils
from gaarf.executors import bq_executor
from gaarf.io import reader
from gaarf.io.writers import bigquery_writer
from googleapiclient import discovery, errors

_SCRIPT_PATH = os.path.dirname(__file__)


class YouTubeDataApiResponse(TypedDict):
  """Represents information on multiple videos in YouTube Data API response."""

  items: list[YouTubeDataApiResponseItem]


class YouTubeDataApiResponseItem(TypedDict):
  """Represents information on a single video in YouTube Data API response."""

  id: str
  fileDetails: dict[str, YouTubeDataApiResponseVideoStreams]  # noqa: N815


class YouTubeDataApiResponseVideoStreams(TypedDict):
  """Represents video streams for a single video."""

  videoStreams: list[dict[str, str | float | int]]  # noqa: N815


@dataclasses.dataclass
class CustomVideoOrientationRegexp:
  """Contains regexp expressions for parsing orientation from video name."""

  width_expression: str | None = None
  height_expression: str | None = None

  def __bool__(self) -> bool:
    """Checks that all attributes are non-empty."""
    return bool(self.width_expression and self.height_expression)


@dataclasses.dataclass
class VideoOrientationRegexp:
  """Contains elements for parsing orientation from video name.

  Attributes:
    element_delimiter: Delimiter to separate various elements in asset name.
    orientation_position: Zero-based position for locating orientation group.
    orientation_delimiter: Delimiter to separate width and height of video.
  """

  element_delimiter: str | None = None
  orientation_position: int | None = None
  orientation_delimiter: str | None = None

  def __bool__(self) -> bool:
    """Checks that all attributes are non-empty."""
    return bool(
      self.element_delimiter
      and self.orientation_position is not None
      and self.orientation_delimiter
    )


def update_config(
  path: str,
  mode: str,
  youtube_config_path: str | None = None,
  video_orientation_regexp: VideoOrientationRegexp
  | CustomVideoOrientationRegexp
  | None = None,
) -> None:
  """Helper methods for saving values to config.

  Args:
    path: Config path.
    mode: Video orientation parsing mode ('youtube', 'regex', 'placeholders').
    youtube_config_path: Path to config to access Youtube Data API.
    video_orientation_regexp: Regexp helper to perform parsing from name.

  Raises:
    ValueError: When there's mismatch between mode and supplied parameters.
  """
  if os.path.exists(path):
    with smart_open.open(path, 'r', encoding='utf-8') as f:
      config = yaml.safe_load(f)
  else:
    config = {}
  scripts_config = {'scripts': {'video_orientation': {'mode': mode}}}
  if mode == 'placeholders':
    config.update(scripts_config)
  elif mode in ('regex', 'custom_regex'):
    if video_orientation_regexp:
      scripts_config['scripts']['video_orientation'].update(
        dataclasses.asdict(video_orientation_regexp)
      )
      config.update(scripts_config)
    else:
      raise ValueError(f'Incorrect regex values provided for {mode} mode.')
  elif mode == 'youtube':
    if youtube_config_path:
      scripts_config['scripts']['video_orientation'].update(
        {'youtube_config_path': youtube_config_path}
      )
      config.update(scripts_config)
    else:
      raise ValueError('No youtube_config_path provided.')
  with smart_open.open(path, 'w', encoding='utf-8') as f:
    yaml.dump(
      config, f, default_flow_style=False, sort_keys=False, encoding='utf-8'
    )


class YouTubeDataConnector:
  """Helper class to get video orientation from YouTube Data API."""

  def __init__(
    self,
    credentials: google.oauth2.credentials.Credentials,
    api_version: str = 'v3',
  ) -> None:
    """Initializes YouTubeDataConnector with credentials and api version.

    Args:
      credentials: Credentials object to get data from YouTube Data API.
      api_version: Version of API to build the service.
    """
    self._service = discovery.build(
      'youtube', api_version, credentials=credentials
    )

  def get_video_orientations(
    self, video_ids: set[str]
  ) -> gaarf.report.GaarfReport:
    """Gets orientation for provided videos.

    Args:
      video_ids: Videos to get orientation for.

    Returns:
      Report with mapping of video_id to its orientation.
    """
    videos_batch = []
    video_orientations = []
    for i, video_id in enumerate(video_ids, start=1):
      videos_batch.append(video_id)
      if i % 50 == 0:
        response = self._get_api_response(','.join(videos_batch))
        video_orientations.extend(self._parse_video_orientation(response))
        videos_batch.clear()
    if videos_batch:
      response = self._get_api_response(','.join(videos_batch))
      video_orientations.extend(self._parse_video_orientation(response))
    return gaarf.report.GaarfReport(
      results=video_orientations, column_names=['video_id', 'video_orientation']
    )

  def _get_api_response(self, video_ids: str) -> YouTubeDataApiResponse:
    return dict(
      self._service.videos().list(part='fileDetails', id=video_ids).execute()
    )

  def _parse_video_orientation(
    self, response: YouTubeDataApiResponse
  ) -> list[tuple[str, str]]:
    """Calls API and converts response to an orientation.

    Args:
      response: Response from YouTube Data API with information on videos.

    Returns:
      Mappings between video_id and its orientation.
    """
    video_orientations = []
    aspect_ratio = None
    if items := response.get('items'):
      for item in items:
        youtube_video_id = item.get('id')
        if video_streams := item.get('fileDetails', {}).get('videoStreams'):
          aspect_ratio = video_streams[0].get('aspectRatio')
        video_orientations.append(
          (youtube_video_id, self._convert_aspect_ratio(aspect_ratio))
        )
    return video_orientations

  def _convert_aspect_ratio(self, aspect_ratio: float | None) -> str:
    """Converts video aspect ratio to one of possible orientation values.

    Args:
      aspect_ratio: Ratio between width and height of a video.

    Returns:
      Video orientation - one of 'Landscape', 'Portrait', 'Square', 'Unknown'.
    """
    if not aspect_ratio:
      return 'Unknown'
    if aspect_ratio > 1:
      return 'Landscape'
    if aspect_ratio < 1:
      return 'Portrait'
    if aspect_ratio == 1:
      return 'Square'
    return 'Unknown'


def generate_placeholders(
  bq_executor: bq_executor.BigQueryExecutor, bq_dataset: str
) -> None:
  """Creates table with 'Unknown' video orientations.

  For all videos that are present in the accounts assign 'Unknown' orientation.

  Args:
    bq_executor: Executor responsible for writing data to BigQuery.
    bq_dataset: BigQuery dtaset to write data to.
  """
  placeholder_query = f"""
    CREATE OR REPLACE TABLE `{bq_dataset}.video_orientation` AS
    SELECT DISTINCT
        video_id,
        "Unknown" AS video_orientation
    FROM `{bq_dataset}.mediafile`
    WHERE type = "VIDEO"
    """
  bq_executor.execute('video_orientation', placeholder_query)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-m', '--mode', dest='mode', default='placeholders')
  parser.add_argument('-c', '--config', dest='gaarf_config', default=None)
  parser.add_argument('--log', '--loglevel', dest='loglevel', default='info')
  parser.add_argument('--logger', dest='logger', default='local')
  parser.add_argument('--ads-config', dest='ads_config')
  parser.add_argument('--save-config', dest='save_config', action='store_true')
  parser.add_argument(
    '--config-destination', dest='save_config_dest', default='config.yaml'
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
  args, kwargs = parser.parse_known_args()

  logger = gaarf_utils.init_logging(
    loglevel=args.loglevel.upper(), logger_type=args.logger
  )

  mode, youtube_config_path, video_orientation_regexp = (
    infer_video_orientation_from_config(**vars(args))
  )
  if args.save_config:
    update_config(
      path=args.gaarf_config,
      mode=mode or args.mode,
      youtube_config_path=youtube_config_path,
      video_orientation_regexp=video_orientation_regexp,
    )
    if args.dry_run:
      exit()

  config = gaarf_utils.ConfigBuilder('gaarf').build(vars(args), kwargs)
  bq_project = config.writer_params.get('project')
  bq_dataset = config.writer_params.get('dataset')
  bigquery_executor = bq_executor.BigQueryExecutor(bq_project)
  bq_writer = bigquery_writer.BigQueryWriter(
    project=bq_project,
    dataset=bq_dataset,
  )
  if mode == 'youtube':
    report_fetcher = gaarf.report_fetcher.AdsReportFetcher(
      api_clients.GoogleAdsApiClient(
        path_to_config=args.ads_config, version=config.api_version
      )
    )
    customer_ids = report_fetcher.expand_mcc(
      config.account, config.customer_ids_query
    )
    videos = set(
      report_fetcher.fetch(
        """SELECT
        media_file.video.youtube_video_id AS video_id
      FROM media_file
      WHERE media_file.type = VIDEO
      """,
        customer_ids,
      ).to_list(row_type='scalar', distinct=True)
    )
    parsed_videos = get_already_parsed_videos(bigquery_executor, bq_dataset)
    videos = videos.difference(parsed_videos) if parsed_videos else videos
    if video_orientations := get_orientation_from_youtube(
      videos,
      args.youtube_config_path or youtube_config_path,
    ):
      bq_writer.write(video_orientations, 'video_orientation')
    else:
      generate_placeholders(bigquery_executor, bq_dataset)

  elif mode in ('regex', 'custom_regex'):
    if mode == 'regex':
      logger.info('Parsing video orientation from asset name based on regexp')
      regexp_video_orientation_query = reader.FileReader().read(
        os.path.join(_SCRIPT_PATH, 'src/video_orientation.sql')
      )
    else:
      logger.info(
        'Parsing video orientation from asset name based on custom regexp'
      )
      regexp_video_orientation_query = reader.FileReader().read(
        os.path.join(_SCRIPT_PATH, 'src/video_orientation_custom_regexp.sql')
      )
    regexp_query_parameters = {
      'macro': {
        'bq_dataset': bq_dataset,
        **dataclasses.asdict(video_orientation_regexp),
      }
    }

    bigquery_executor.execute(
      'video_orientation',
      regexp_video_orientation_query,
      regexp_query_parameters,
    )
  else:
    logger.info('Generating placeholders for video orientation')
    mode = 'placeholders'
    generate_placeholders(bigquery_executor, bq_dataset)


def get_orientation_from_youtube(
  videos: set[str],
  youtube_config_path: str,
) -> gaarf.report.GaarfReport | None:
  """Gets orientation for videos from YouTube Data API.

  Args:
    videos: Video ids to get orientation for.
    youtube_config_path: Path to config to authenticate API access.

  Returns:
    Report with mappings between video_id and its orientation.
  """
  logging.info('Getting video orientation from YouTube')
  with smart_open.open(youtube_config_path, 'r', encoding='utf-8') as f:
    youtube_config = yaml.safe_load(f)
  credentials = google.oauth2.credentials.Credentials(
    None,
    refresh_token=youtube_config.get('refresh_token'),
    token_uri='https://oauth2.googleapis.com/token',
    client_id=youtube_config.get('client_id'),
    client_secret=youtube_config.get('client_secret'),
  )
  youtube_data_connector = YouTubeDataConnector(credentials)
  try:
    return youtube_data_connector.get_video_orientations(videos)
  except errors.HttpError:
    logging.warning('Unable to access YouTube Data API, using placeholders')
    return None
  else:
    logging.info('No new videos to parse')
    return None


def get_already_parsed_videos(
  bigquery_executor: bq_executor.BigQueryExecutor, bq_dataset: str
) -> set[str]:
  """Gets information on already inferred video orientations."""
  try:
    parsed_videos = bigquery_executor.execute(
      script_name='existing_videos',
      query_text=f"""
                SELECT DISTINCT
                  video_id
                FROM {bq_dataset}.video_orientation
                """,
    )
    return set(parsed_videos['video_id'])
  except bq_executor.BigQueryExecutorException:
    return set()


def infer_video_orientation_from_config(  # noqa: PLR0913
  gaarf_config: str,
  mode: str | None = None,
  youtube_config_path: str | None = None,
  element_delimiter: str | None = None,
  orientation_position: int | None = None,
  orientation_delimiter: str | None = None,
  width_expression: str | None = None,
  height_expression: str | None = None,
  **kwargs: str,  # noqa: ARG001
) -> tuple[str, str, VideoOrientationRegexp | CustomVideoOrientationRegexp]:
  """Infer necessary parameters for getting video orientations.

  Args:
    gaarf_config:  Configuration file to interact with BigQuery and Ads API.
    mode: Video orientation parsing mode ('youtube', 'regex', 'placeholders').
    youtube_config_path: Path to config to access Youtube Data API.
    element_delimiter: Delimiter to separate various elements in asset name.
    orientation_position: Zero-based position for locating orientation group.
    orientation_delimiter: Delimiter to separate width and height of video.
    width_expression: Regular expression for finding width of video.
    height_expression: Regular expression for finding height of video.
    kwargs: Keywords arguments that can be passed to function, ignored.

  Returns:
    Mode, optional path to YouTube config and video orientation regexp.
  """
  with smart_open.open(gaarf_config, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)
    if video_orientation_config := config.get('scripts', {}).get(
      'video_orientation'
    ):
      mode = mode or video_orientation_config.get('mode')
      element_delimiter = element_delimiter or video_orientation_config.get(
        'element_delimiter'
      )
      orientation_position = (
        orientation_position
        or video_orientation_config.get('orientation_position')
      )
      orientation_delimiter = (
        orientation_delimiter
        or video_orientation_config.get('orientation_delimiter')
      )
      youtube_config_path = youtube_config_path or video_orientation_config.get(
        'youtube_config_path'
      )
      width_expression = width_expression or video_orientation_config.get(
        'width_expression'
      )
      height_expression = height_expression or video_orientation_config.get(
        'height_expression'
      )
  video_orientation_regexp = VideoOrientationRegexp(
    element_delimiter, orientation_position, orientation_delimiter
  )
  custom_video_orientation_regexp = CustomVideoOrientationRegexp(
    width_expression, height_expression
  )
  return (
    mode,
    youtube_config_path,
    video_orientation_regexp or custom_video_orientation_regexp,
  )


if __name__ == '__main__':
  main()
