from functools import partial
from typing import Dict, List
from datetime import datetime, timedelta

from airflow.models import DAG, Variable


def get_env_params(
        bq_dataset_prod: str = 'raw_data',
        bq_dataset_stg: str = 'stg_data',
        bq_project_id: str = 'p2p-data-warehouse',
        gcs_bucket_prod: str = 'data-etl-storage',
        gcs_bucket_stg: str = 'data-stg-etl-storage',

) -> Dict:
    """
    Airflow Variable:
    env_params_prod = {
        "env_mode": "prod",
        "airflow_home_dir": "/home/airflow/data",
        "gcp_conn_id": "de",
        "dbt_bin": "/home/airflow/.env/airflow_env/bin/dbt",
        "dbt_profiles_dir": "/home/airflow/.dbt",
        "dbt_project_dir": "/home/airflow/data/p2p"
    }
    env_params_local = {
        "env_mode": "stg",
        "airflow_home_dir": "/opt/airflow",
        "airflow_output": "/tmp",
        "gcp_conn_id": "de",
        "slack_alerting_channel": "de_alerting_stage"
        "dbt_bin": "/home/airflow/.local/bin/dbt",
        "dbt_profiles_dir": "/opt/airflow",
        "dbt_project_dir": "/opt/airflow/p2p"
    }
    env_params_k8s = {
        "env_mode": "stg",
        "airflow_home_dir": "/opt/airflow/dags/repo",
        "airflow_output": "/tmp",
        "gcp_conn_id": "de",
        "slack_alerting_channel": "de_alerting_stage"
        "dbt_bin": "/home/airflow/.local/bin/dbt",
        "dbt_profiles_dir": "/opt/airflow/dags/repo/infra/dbt_profiles/stage",
        "dbt_project_dir": "/opt/airflow/dags/repo/p2p",
    }
    """
    # load params dict from Airflow Variables
    env_params = Variable.get('airflow_env_params', deserialize_json=True)
    env_mode = env_params['env_mode']


    output = {
        # airflow mode [prod/stg]
        'env_mode': env_mode,
        'airflow_home_dir': env_params['airflow_home_dir'],
        'airflow_output': env_params['airflow_output'],
        # gcp/bq
        'gcp_conn_id': env_params['gcp_conn_id'],
        'bucket_name': gcs_bucket_prod if env_mode == 'prod' else gcs_bucket_stg,
        'bq_project_id': env_params['bq_project_id'],
        'bq_dataset': env_params['bq_dataset'],
        # dbt
        'dbt_bin': env_params['dbt_bin'],
        'dbt_project_dir': env_params['dbt_project_dir'],
        'dbt_profiles_dir': env_params['dbt_profiles_dir'],
        'dbt_target': env_mode,
    }
    return output


def create_dag(
        owner: str,
        dag_id: str,
        tags: List[str],
        on_failure_callback_function: partial = None,
        schedule_interval: str = None,
        description: str = None,
        retries: int = 1,
        retry_delay: timedelta = timedelta(minutes=3),
        start_date: datetime = datetime(2023, 1, 1),
        execution_timeout: timedelta = timedelta(minutes=100),
        dagrun_timeout: timedelta = timedelta(minutes=100),
        sla: timedelta = timedelta(minutes=100),
        depends_on_past: bool = False,
        catchup: bool = False,
        max_active_runs: int = 1,
        concurrency: int = None,
        pool: str = None,
        params: dict = None,
        # dbt_operator
        dbt_project_dir: str = None,
        dbt_profiles_dir: str = None,
        dbt_bin: str = None,
        dbt_target: str = None,
) -> DAG:
    """

    :param owner: owner of the DAG
    :param dag_id: unique dag name
    :param tags: tags for search in webUI
    :param on_failure_callback_function: callback function for slack alerting
    :param schedule_interval: cron run schedule
    :param description: The description for the DAG to e.g. be shown on the webserver
    :param retries:  the number of retries that should be performed before failing the task. default=1
    :param retry_delay: delay between retries. default=3minutes
    :param start_date: The timestamp from which the scheduler will attempt to backfill
    :param execution_timeout: max time allowed for the execution of this TaskInstance,
    if it goes beyond it will raise and fail. default=100minutes
    :param dagrun_timeout: specify how long a DagRun should be up before timing out / failing,
    so that new DagRuns can be created. default=100minutes
    :param sla: An SLA, or a Service Level Agreement, is an expectation for the maximum time a Task
    should be completed relative to the Dag Run start time. If a task takes longer than this to run,
    it is then visible in the “SLA Misses” part of the user interface. default=100minutes
    :param depends_on_past:  when set to true, task instances will run sequentially while relying
    on the previous task’s schedule to succeed. The task instance for the start_date is allowed to run.
    :param catchup: Perform scheduler catchup (or only run latest)? default=False
    :param max_active_runs: maximum number of active DAG runs, beyond this number of DAG runs in a running state,
    the scheduler won't create new active DAG run. default=1
    :param concurrency: maximum number of task instances allowed to run concurrently
    across all active DAG runs for a given DAG
    :param pool: can be used to limit the execution parallelism on arbitrary sets of tasks.
    :param params:
    :param dbt_project_dir:
    :param dbt_profiles_dir:
    :param dbt_bin:
    :param dbt_target:
    :return:
    """
    # env params
    env_params = get_env_params()

    default_args = {
        # airflow dag/tasks
        'owner': owner,
        'on_failure_callback': on_failure_callback_function,
        'sla': sla,
        'execution_timeout': execution_timeout,
        'depends_on_past': depends_on_past,
        'retries': retries,
        'retry_delay': retry_delay,
        'pool': pool,
        # dbt_operator
        'dir': dbt_project_dir if dbt_project_dir else env_params['dbt_project_dir'],
        'profiles_dir': dbt_profiles_dir if dbt_profiles_dir else env_params['dbt_profiles_dir'],
        'dbt_bin': dbt_bin if dbt_bin else env_params['dbt_bin'],
        'target': dbt_target if dbt_target else env_params['dbt_target'],
    }
    if params:
        default_args['params'] = params

    return DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=start_date,
        description=description,
        schedule_interval=schedule_interval,
        catchup=catchup,
        max_active_runs=max_active_runs,
        dagrun_timeout=dagrun_timeout,
        concurrency=concurrency,
        tags=tags,
        doc_md=__doc__,
    )
