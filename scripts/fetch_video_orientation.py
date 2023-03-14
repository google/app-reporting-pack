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

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import argparse
import os
import logging
import google
from rich.logging import RichHandler
import yaml
from googleapiclient.discovery import build

from gaarf.api_clients import GoogleAdsApiClient
from gaarf.bq_executor import BigQueryExecutor
from gaarf.utils import get_customer_ids
from gaarf.query_executor import AdsReportFetcher
from gaarf.cli.utils import GaarfConfigBuilder, GaarfBqConfigBuilder
from gaarf.io.reader import FileReader
from gaarf.io.writer import WriterFactory
from gaarf.report import GaarfReport

import src.queries as queries


@dataclass
class VideoOrientationConfig:
    mode: str
    params: Dict[str, Any]


@dataclass
class VideoOrientationRegexp:
    element_delimiter: Optional[str] = None
    orientation_position: Optional[int] = None
    orientation_delimiter: Optional[str] = None


def update_config(
        path: str,
        mode: str,
        video_orientation_regexp: Optional[VideoOrientationRegexp] = None):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    else:
        config = {}
    scripts_config = {"scripts": {"video_orientation": {"mode": mode}}}
    if video_orientation_regexp:
        scripts_config["scripts"]["video_orientation"].update(
            asdict(video_orientation_regexp))
    config.update(scripts_config)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(config,
                  f,
                  default_flow_style=False,
                  sort_keys=False,
                  encoding="utf-8")


class YouTubeDataConnector:

    def __init__(self, credentials, api_version: str = "v3") -> None:
        self.service = build("youtube", api_version, credentials=credentials)

    def get_response(self, elements: str) -> List[Tuple[str, Optional[float]]]:
        videos = []
        video_orientations = []
        for i, element in enumerate(elements):
            videos.append(element)
            if i % 50 == 0:
                video_orientations.extend(
                    self._parse_video_orientation(",".join(videos)))
                videos.clear()
        if videos:
            video_orientations.extend(
                self._parse_video_orientation(",".join(videos)))
        return GaarfReport(results=video_orientations,
                           column_names=["video_id", "video_orientation"])

    def _parse_video_orientation(
            self, videos: str) -> List[Tuple[str, Optional[str]]]:
        video_orientations = []
        response = self.service.videos().list(part="fileDetails",
                                              id=videos).execute()
        aspect_ratio = None
        if items := response.get("items"):
            for item in items:
                youtube_video_id = item.get("id")
                if file_details := item.get("fileDetails"):
                    if video_streams := file_details.get("videoStreams"):
                        aspect_ratio = video_streams[0].get("aspectRatio")
                video_orientations.append(
                    (youtube_video_id,
                     self._convert_aspect_ratio(aspect_ratio)))
        return video_orientations

    def _convert_aspect_ratio(self, aspect_ratio: Optional[float]) -> str:
        if not aspect_ratio:
            return "Unknown"
        if aspect_ratio > 1:
            return "Landscape"
        if aspect_ratio < 1:
            return "Portrait"
        if aspect_ratio == 1:
            return "Square"
        return "Unknown"


def generate_placeholders(bq_executor, config) -> None:
    placeholder_query = """
            CREATE OR REPLACE TABLE `{bq_dataset}.video_orientation` AS
            SELECT DISTINCT
                video_id,
                "Unknown" AS video_orientation
            FROM `{bq_dataset}.mediafile`
            WHERE type = "VIDEO"
        """
    bq_executor.execute("video_orientation", placeholder_query, config.params)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", dest="mode", default="placeholders")
    parser.add_argument("-c", "--config", dest="gaarf_config", default=None)
    parser.add_argument("--output", dest="save", default="bq")
    parser.add_argument("--ads-config", dest="ads_config")
    parser.add_argument("--account", dest="customer_id")
    parser.add_argument("--api-version", dest="api_version", default="12")
    parser.add_argument("--log", "--loglevel", dest="loglevel", default="info")
    parser.add_argument("--customer-ids-query",
                        dest="customer_ids_query",
                        default=None)
    parser.add_argument("--customer-ids-query-file",
                        dest="customer_ids_query_file",
                        default=None)

    parser.add_argument("--save-config",
                        dest="save_config",
                        action="store_true")
    parser.add_argument("--config-destination",
                        dest="save_config_dest",
                        default="config.yaml")

    parser.add_argument("--project", dest="project", default=None)
    parser.add_argument("--dataset-location",
                        dest="dataset_location",
                        default=None)
    parser.add_argument("--element-delimiter",
                        dest="element_delimiter",
                        default=None)
    parser.add_argument("--orientation-position",
                        dest="orientation_position",
                        default=None)
    parser.add_argument("--orientation-delimiter",
                        dest="orientation_delimiter",
                        default=None)
    parser.set_defaults(save_config=False)
    args = parser.parse_known_args()

    logging.basicConfig(format="%(message)s",
                        level=args[0].loglevel.upper(),
                        datefmt="%Y-%m-%d %H:%M:%S",
                        handlers=[RichHandler(rich_tracebacks=True)])
    logging.getLogger("google.ads.googleads.client").setLevel(logging.WARNING)
    logging.getLogger("smart_open.smart_open_lib").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)

    mode = args[0].mode
    if args[0].gaarf_config:
        with open(args[0].gaarf_config, "r", encoding="utf-8") as f:
            gaarf_config = yaml.safe_load(f)
            if scripts := gaarf_config.get("scripts"):
                video_orientation_config = scripts.get("video_orientation")
                mode = video_orientation_config.get("mode")
                element_delimiter = video_orientation_config.get(
                    "element_delimiter")
                orientation_position = video_orientation_config.get(
                    "orientation_position")
                orientation_delimiter = video_orientation_config.get(
                    "orientation_delimiter")
            else:
                element_delimiter = None
                orientation_position = None
                orientation_delimiter = None

    bq_config = GaarfBqConfigBuilder(args).build()
    bq_executor = BigQueryExecutor(bq_config.project)
    if mode == "youtube":
        logger.info("Getting video orientation from YouTube")
        try:
            parsed_videos = bq_executor.execute(script_name="existing_videos",
                                                query_text="""
                SELECT DISTINCT
                    video_id,
                    video_orientation
                FROM {bq_dataset}.video_orientation
                """,
                                                params=bq_config.params)
        except Exception as e:
            parsed_videos = set()
        with open(args[0].ads_config, "r", encoding="utf-8") as f:
            google_ads_config_dict = yaml.safe_load(f)
        config = GaarfConfigBuilder(args).build()
        google_ads_client = GoogleAdsApiClient(
            config_dict=google_ads_config_dict,
            version=f"v{config.api_version}")
        customer_ids = get_customer_ids(google_ads_client, config.account,
                                        config.customer_ids_query)
        report_fetcher = AdsReportFetcher(google_ads_client, customer_ids)
        videos = report_fetcher.fetch(queries.Videos()).to_list()
        if parsed_videos:
            videos = list(
                set(videos).difference(set(parsed_videos["video_id"])))
        else:
            videos = list(set(videos))
        if videos:
            credentials = google.oauth2.credentials.Credentials(
                None,
                refresh_token=google_ads_config_dict.get("yt_refresh_token"),
                token_uri="https://oauth2.googleapis.com/token",
                client_id=google_ads_config_dict.get("client_id"),
                client_secret=google_ads_config_dict.get("client_secret"))
            youtube_data_connector = YouTubeDataConnector(credentials)
            try:
                video_orientations = youtube_data_connector.get_response(
                    videos)
                writer_client = WriterFactory().create_writer(
                    config.output, **config.writer_params)
                writer_client.write(video_orientations, "video_orientation")
            except Exception as e:
                logger.warning(
                    "Unable to access YouTube Data API, generating placeholders"
                )
                generate_placeholders(bq_executor, bq_config)
                mode = "placeholders"
        else:
            logger.info("No new videos to parse")

    elif mode == "regex":
        logger.info("Parsing video orientation from asset name based on regexp")
        script_path = os.path.dirname(__file__)
        video_orientation_regexp = VideoOrientationRegexp(
            element_delimiter=args[0].element_delimiter,
            orientation_position=args[0].orientation_position,
            orientation_delimiter=args[0].orientation_delimiter)
        bq_executor.execute(
            "video_orientation",
            FileReader().read(
                os.path.join(script_path, "src/video_orientation.sql")
            ), {
                "macro": {
                    "bq_dataset":
                    bq_config.params.get("macro").get("bq_dataset"),
                    "element_delimiter":
                    args[0].element_delimiter or element_delimiter,
                    "orientation_position":
                    args[0].orientation_position or orientation_position,
                    "orientation_delimiter":
                    args[0].orientation_delimiter or orientation_position
                }
            })
    else:
        logger.info("Generating placeholders for video orientation")
        mode = "placeholders"
        video_orientation_regexp = None
    if args[0].save_config:
        update_config(path=args[0].gaarf_config,
                      mode=mode,
                      video_orientation_regexp=video_orientation_regexp)


if __name__ == "__main__":
    main()
