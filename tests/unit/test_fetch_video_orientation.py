# Copyright 2024 Google LLC
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

import os
import pathlib

import gaarf
import pytest
import yaml
from scripts import fetch_video_orientation

_SCRIPT_PATH = os.path.dirname(__file__)


class TestYouTubeDataConnector:
  @pytest.fixture
  def youtube_data_connector(self):
    return fetch_video_orientation.YouTubeDataConnector(credentials=None)

  def test_get_video_orientations_returns_correct_results(
    self, youtube_data_connector, mocker
  ):
    mocker.patch(
      'scripts.fetch_video_orientation.YouTubeDataConnector._get_api_response',
      return_value={
        'items': [
          {
            'id': 'video1',
            'fileDetails': {'videoStreams': [{'aspectRatio': 1.87}]},
          },
          {
            'id': 'video2',
            'fileDetails': {'videoStreams': [{'aspectRatio': 1.00}]},
          },
          {
            'id': 'video3',
            'fileDetails': {'videoStreams': [{'aspectRatio': 0.87}]},
          },
        ]
      },
    )
    results = youtube_data_connector.get_video_orientations(
      ['video1', 'video2', 'video3']
    )

    expected_results = gaarf.report.GaarfReport(
      results=[
        ('video1', 'Landscape'),
        ('video2', 'Square'),
        ('video3', 'Portrait'),
      ],
      column_names=['video_id', 'video_orientation'],
    )

    assert results == expected_results


def test_update_config_saves_results_for_youtube_mode(tmp_path):
  config_path = tmp_path / 'config.yaml'
  fetch_video_orientation.update_config(
    config_path, mode='youtube', youtube_config_path=str(config_path)
  )

  with open(config_path, 'r', encoding='utf-8') as f:
    results = yaml.safe_load(f)
    expected_results = {
      'scripts': {
        'video_orientation': {
          'mode': 'youtube',
          'youtube_config_path': str(config_path),
        }
      }
    }

  assert results == expected_results


def test_update_config_saves_results_for_regex_mode(tmp_path):
  config_path = tmp_path / 'config.yaml'
  video_orientation_regexp = fetch_video_orientation.VideoOrientationRegexp(
    element_delimiter='_', orientation_position=1, orientation_delimiter='x'
  )
  fetch_video_orientation.update_config(
    config_path, mode='regex', video_orientation_regexp=video_orientation_regexp
  )

  with open(config_path, 'r', encoding='utf-8') as f:
    results = yaml.safe_load(f)
    expected_results = {
      'scripts': {
        'video_orientation': {
          'mode': 'regex',
          'element_delimiter': '_',
          'orientation_position': 1,
          'orientation_delimiter': 'x',
        }
      }
    }

  assert results == expected_results


def test_update_config_saves_results_for_custom_regex_mode(tmp_path):
  config_path = tmp_path / 'config.yaml'
  video_orientation_regexp = (
    fetch_video_orientation.CustomVideoOrientationRegexp(
      width_expression='x',
      height_expression='-',
    )
  )
  fetch_video_orientation.update_config(
    config_path,
    mode='custom_regex',
    video_orientation_regexp=video_orientation_regexp,
  )

  with open(config_path, 'r', encoding='utf-8') as f:
    results = yaml.safe_load(f)
    expected_results = {
      'scripts': {
        'video_orientation': {
          'mode': 'custom_regex',
          'width_expression': 'x',
          'height_expression': '-',
        }
      }
    }

  assert results == expected_results


def test_infer_video_orientation_from_config():
  config_path = os.path.join(_SCRIPT_PATH, 'data/config_without_scripts.yaml')
  mode, youtube_config_path, video_orientation_regexp = (
    fetch_video_orientation.infer_video_orientation_from_config(config_path)
  )

  assert not mode
  assert not youtube_config_path
  assert not video_orientation_regexp


def test_infer_video_orientation_from_config_with_youtube():
  config_path = os.path.join(_SCRIPT_PATH, 'data/config_youtube.yaml')
  mode, youtube_config_path, video_orientation_regexp = (
    fetch_video_orientation.infer_video_orientation_from_config(config_path)
  )

  assert mode == 'youtube'
  assert youtube_config_path == 'gs://fake-bucket/config.yaml'
  assert not video_orientation_regexp


def test_infer_video_orientation_from_config_with_regexp():
  config_path = os.path.join(_SCRIPT_PATH, 'data/config_regexp.yaml')
  mode, youtube_config_path, video_orientation_regexp = (
    fetch_video_orientation.infer_video_orientation_from_config(config_path)
  )

  assert mode == 'regex'
  assert (
    video_orientation_regexp
    == fetch_video_orientation.VideoOrientationRegexp(
      element_delimiter='_', orientation_position=1, orientation_delimiter='x'
    )
  )

  assert not youtube_config_path


def test_infer_video_orientation_from_config_with_custom_regexp():
  config_path = os.path.join(_SCRIPT_PATH, 'data/config_custom_regexp.yaml')
  mode, youtube_config_path, video_orientation_regexp = (
    fetch_video_orientation.infer_video_orientation_from_config(config_path)
  )

  assert mode == 'custom_regex'
  assert video_orientation_regexp == (
    fetch_video_orientation.CustomVideoOrientationRegexp(
      width_expression='x',
      height_expression='-',
    )
  )

  assert not youtube_config_path
