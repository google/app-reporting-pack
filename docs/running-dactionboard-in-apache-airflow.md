# Running App Reporting Pack in Apache Airflow


Running App Reporting Pack queries in Apache Airflow is easy.
You'll need to provide three arguments for running `DockerOperator` inside your DAG:

* `/path/to/google-ads.yaml` - absolute path to `google-ads.yaml` file (can be remote)
* `service_account.json` - absolute path to service account json file
* `/path/to/app_reporting_pack.yaml` - absolute path to YAML config.

## Example DAGs

### Getting configuration files locally

> Don't forget to change `/path/to/google-ads.yaml`, `path/to/service_account.json`
> and `path/to/app_reporting_pack.yaml` with valid paths.

```
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


default_args = {
    'description'           : 'https://github.com/google/app-reporting-pack',
    'depend_on_past'        : False,
    'start_date'            : datetime(2023, 3, 1),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 1,
    'retry_delay'           : timedelta(minutes=5)
}
with DAG('app_reporting_pack_local', default_args=default_args, schedule_interval="* 0 * * *", catchup=False) as dag:
    app_reporting_pack = DockerOperator(
        task_id='app_reporting_pack_docker',
        image='ghcr.io/google/app-reporting-pack:latest',
        api_version='auto',
        auto_remove=True,
        command=[
            "-g", "/google-ads.yaml",
            "-c", "/app_reporting_pack.yaml",
            "--legacy"
        ],
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(
                source="/path/to/service_account.json",
                target="/service_account.json",
                type="bind"),
            Mount(
                source="/path/to/google-ads.yaml",
                target="/google-ads.yaml",
                type="bind"),
            Mount(
                source="/path/to/app_reporting_pack.yaml",
                target="/app_reporting_pack.yaml",
                type="bind")
        ]
    )
    app_reporting_pack
```


### Getting configuration files from Google Cloud Storage

> Don't forget to change `gs://path/to/google-ads.yaml`, `path/to/service_account.json`
> and `gs://path/to/app_reporting_pack.yaml` with valid paths.
```
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


default_args = {
    'description'           : 'https://github.com/google/app-reporting-pack',
    'depend_on_past'        : False,
    'start_date'            : datetime(2023, 3, 1),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 1,
    'retry_delay'           : timedelta(minutes=5)
}
with DAG('app_reporting_pack_remote', default_args=default_args, schedule_interval="* 0 * * *", catchup=False) as dag:
    app_reporting_pack = DockerOperator(
        task_id='app_reporting_pack_docker',
        image='ghcr.io/google/app-reporting-pack:latest',
        api_version='auto',
        auto_remove=True,
        command=[
            "-g", "gs://path/to//google-ads.yaml",
            "-c", "gs://path/to/app_reporting_pack.yaml",
            "--legacy"
        ],
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(
                source="/path/to/service_account.json",
                target="/service_account.json",
                type="bind")
    )
    app_reporting_pack
```

