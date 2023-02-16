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

from google.cloud import bigquery


def write_data_to_bq(bq_client: bigquery.Client,
                     data,
                     table_id: str,
                     write_disposition: str = "WRITE_TRUNCATE") -> None:
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, )
    job = bq_client.load_table_from_dataframe(data,
                                              table_id,
                                              job_config=job_config)
    job.result()
