import os
import subprocess
import yaml
import urllib.parse
from flask import Flask, request, redirect

_DATASOURCES_DICT = {
    "excellence": ("creative_excellence", "UAC Hygiene"),
    "perf_group": ("performance_grouping", "Performance Grouping Changes"),
    "network": ("ad_group_network_split", "Network Splits"),
    "assets": ("asset_performance", "Assets"),
    "approvals": ("approval_statuses", "Disapprovals"),
    "changes": ("change_history", "Final Change History")
}

_REPORT_ID = "187f1f41-16bc-434d-8437-7988bed6e8b9"
_REPORT_NAME = "New Report"
_DATASET_ID = "app_reporting_pack_target"
_BASE_URL = "https://datastudio.google.com/reporting/create?"
_CONFIG_FILE_PATH = "./config.yaml"


app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    """Create a Looker dashboard creation link with the Linking API, and return a button
    that redirects to it."""

    with open(_CONFIG_FILE_PATH, 'r') as f:
        config_data = yaml.load(f, Loader=yaml.FullLoader)
        gaarf_data = config_data.get('gaarf')
        bq_data = gaarf_data.get('bq')

    project_id = bq_data.get('project')

    dashboard_url = create_url(_REPORT_NAME, _REPORT_ID, project_id, _DATASET_ID, _DATASOURCES_DICT)
    return f"""<!DOCTYPE html>
                <html>
                    <head>
                        <title>App Reporting Pack</title>
                    </head>
                    <p>Click on "Run Queries" to manually trigger the queries. </br>Click on "Create Dashboard" to create your private copy of App Reporting Pack dashboard.<p>
                    <body>
                        <button onclick="window.location.href='run-queries';alert('Running Queries')">
                            Run Queries
                        </button>
                    </body>
                    <body>
                        <button onclick="window.location.href='{dashboard_url}';">
                            Create Dashboard
                        </button>
                    </body>
                </html>"""


@app.route("/run-queries", methods=["POST", "GET"])
def run_queries():
    """Run the App Reporting Pack queries and save results to BQ."""
    print("Request recieved. Running queries")
    try:
        subprocess.check_call(["./run-docker.sh", "google_ads_queries/*/*.sql", "bq_queries", "/google-ads.yaml"])
        return ("", 204)
    except Exception as e:
        print("Failed running queries", str(e))
        return ("Failed running queries", 400)



def create_url(report_name, report_id, project_id, dataset_id, _DATASOURCES_DICT):
    url = _BASE_URL
    url_safe_report_name = urllib.parse.quote(report_name)

    url += f"c.mode=edit&c.reportId={report_id}&r.reportName={url_safe_report_name}&ds.*.refreshFields=false&"

    for ds_num, ds_data in _DATASOURCES_DICT.items():
        url += create_datasource(project_id, dataset_id, ds_num, ds_data[0], ds_data[1])

    return url[:-1]  # remove last ""&""


def create_datasource(project_id, dataset_id, ds_num, table_id, datasource_name):
    url_safe_name = urllib.parse.quote(datasource_name)

    return (f'ds.{ds_num}.connector=bigQuery'
          f'&ds.{ds_num}.datasourceName={url_safe_name}'
          f'&ds.{ds_num}.projectId={project_id}'
          f'&ds.{ds_num}.type=TABLE'
          f'&ds.{ds_num}.datasetId={dataset_id}'
          f'&ds.{ds_num}.tableId={table_id}&')


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=int(os.environ.get("PORT", 8080)))