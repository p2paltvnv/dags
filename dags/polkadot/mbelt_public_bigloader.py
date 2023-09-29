from datetime import timedelta

from airflow.utils.dates import days_ago
#from dag_utils.slack.slask_alert import task_fail_slack_alert
from airflow.operators.python import PythonOperator
from functools import partial
from dag_utils.bigloader import bigloader as bl
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.empty import EmptyOperator as DummyOperator
from dag_utils.helpers.dags_config import create_dag
from dag_utils.helpers.dags_config import create_dag, get_env_params

env_params = get_env_params()
dag = create_dag(
    dag_id='mbelt3_public_bigloader',
    owner='Ilia Malyshev',
    schedule_interval="50 * * * *",
    tags=['mbelt', 'bigloader'],
    catchup=False,
    start_date=days_ago(0),
    dagrun_timeout=timedelta(minutes=50),
    execution_timeout=timedelta(minutes=25),
    retries=0
)


def branch_metrics(**op_kwargs):
    table = op_kwargs['table'] if 'table' in op_kwargs else None
    finish_name = op_kwargs['finish_name'] if 'finish_name' in op_kwargs else None
    metric_name = op_kwargs['metric_name'] if 'metric_name' in op_kwargs else None

    if table in ['blocks', 'eras', 'rounds']:
        return metric_name
    else:
        return finish_name


for module_nm in [
    'static_files.config_bigloader_mbelt3_polkadot',
    'static_files.config_bigloader_mbelt3_kusama',
    'static_files.config_bigloader_mbelt3_moonriver',
    'static_files.config_bigloader_mbelt3_moonbeam',
]:
    module_nm = f'polkadot.{module_nm}'
    mod = __import__(module_nm, fromlist=[''])

    start_chain = DummyOperator(task_id=f'start_chain_{mod.CHAIN}', trigger_rule=TriggerRule.ALL_DONE)
    start_chain

    for table, params in mod.TABLE_CONF.items():
        start = DummyOperator(task_id=f'start_{mod.CHAIN}_{table}', trigger_rule=TriggerRule.ALL_DONE)
        finish = DummyOperator(task_id=f'finish_{mod.CHAIN}_{table}', trigger_rule=TriggerRule.ALL_DONE)

        download_data = PythonOperator(
            dag=dag,
            task_id=f'download_{mod.CHAIN}_{table}',
            python_callable=bl.extract_data_to_s3,
            op_kwargs={
                'conn_id': f'mbelt3_{mod.CHAIN}',
                'file_dir': f'mbelt3_public_{mod.CHAIN}/',
                'file_name': f'{table}.csv',
                'table_id': f'{env_params["bq_project_id"]}.{env_params["bq_dataset"]}.mbelt3_{mod.CHAIN}__{table}',
                'table_name': f'public.{table}',
                'id_col_name': params['id_col_name'] if 'id_col_name' in params else None,
                'update_type': params['update_type'] if 'update_type' in params else None,
                'batch': 1000000, #params['batch'] if 'batch' in params else 5_000_000,
                'file_size_limit_rows': 1000000,#params[
                    #'file_size_limit_rows'] if 'file_size_limit_rows' in params else 5_000_000,
            },
            provide_context=True)

        create_table = PythonOperator(
            dag=dag,
            task_id=f'create_table_{mod.CHAIN}_{table}',
            python_callable=bl.create_table_bq,
            op_kwargs={
                'prefix': f'mbelt3_{mod.CHAIN}__',
                'file_dir': f'mbelt3_public_{mod.CHAIN}/',
                'file_name': f'{table}.csv',
                'project_name': f'{env_params["bq_project_id"]}',
                'dataset_name': f'{env_params["bq_dataset"]}',
                'table_name': table,
                'schema': params['schema'],
                'write_disposition': params['write_disposition'],
            },
            provide_context=True
        )

        (
                start >>
                download_data >>
                create_table >>
                finish
        )

