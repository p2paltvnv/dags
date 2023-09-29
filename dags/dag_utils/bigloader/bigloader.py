from google.cloud import storage, bigquery
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from tempfile import TemporaryDirectory
import psycopg2
import clickhouse_driver
import os
import logging
import subprocess
from airflow.models import Variable

#DOCS INFO - https://hevodata.com/blog/postgresql-to-bigquery-data-migration/

def get_last_id_bq(table_id, id_col_name):
    creds = Variable.get("sa_bigquery_token", deserialize_json=True)

    client = bigquery.Client.from_service_account_info(creds)
    query_job = client.query(f'''/* {{"app": "python_legacy", "owner": "bigloader"}} */
                                     SELECT max({id_col_name}) as last_id FROM {table_id}''')
    try:
        last_id = int([i.last_id for i in query_job.result()][0])
    except Exception as e:
        last_id = 0
    print('LAST ID ', last_id)

    return last_id+1

# Temp function for Solana upload gaps
def execute_query(query):
    creds = Variable.get("sa_bigquery_token", deserialize_json=True)
    client = bigquery.Client.from_service_account_info(creds)
    query_job = client.query(query)
    result = int([i for i in query_job.result()][0])
    print(result)

def extract_data_to_s3(**op_kwargs):
    conn_id = op_kwargs['conn_id'] if 'conn_id' in op_kwargs else None
    file_dir = op_kwargs['file_dir'] if 'file_dir' in op_kwargs else ''
    file_name = op_kwargs['file_name'] if 'file_name' in op_kwargs else None
    table_id = op_kwargs['table_id'] if 'table_id' in op_kwargs else None
    table_name = op_kwargs['table_name'] if 'table_name' in op_kwargs else None
    source_name = op_kwargs['source_name'] if 'source_name' in op_kwargs else table_name
    columns = op_kwargs['columns'] if 'columns' in op_kwargs else '*'
    query = op_kwargs['query'] if 'query' in op_kwargs else None
    id_col_name = op_kwargs['id_col_name'] if 'id_col_name' in op_kwargs else None
    batch = op_kwargs['batch'] if 'batch' in op_kwargs else 10000000
    update_type = op_kwargs['update_type'] if 'update_type' in op_kwargs else None
    file_size_limit_rows = op_kwargs['file_size_limit_rows'] if 'file_size_limit_rows' in op_kwargs else None
    final = op_kwargs['final'] if 'final' in op_kwargs else ''
    ti = op_kwargs['ti']
    is_balval = op_kwargs['is_balval'] if 'is_balval' in op_kwargs else False

    logical_date = op_kwargs['execution_date']
    logical_date_dir = logical_date.strftime("%Y%m%d")
    logical_date_subdir = logical_date.strftime("%H%M%S")
    file_dir = f'{file_dir}{logical_date_dir}/{logical_date_subdir}/' if len(file_dir) > 0 \
        else f'{logical_date_dir}/{logical_date_subdir}/'
    
    output_dir = str(Variable.get("airflow_env_params", deserialize_json=True)['airflow_output']) + '/'
    
    with TemporaryDirectory(dir=output_dir) as tempdir:
        file_path = os.path.join(tempdir, file_name)
        logging.info(f"output_dir: {file_path}")

        conn_param = BaseHook.get_connection(conn_id)
        conn_type = conn_param.conn_type
    
        if conn_type == 'postgres':
            conn = psycopg2.connect(host=conn_param.host,
                                    port=conn_param.port,
                                    database=conn_param.schema,
                                    user=conn_param.login,
                                    password=conn_param.password)
    
            db_cursor = conn.cursor()
    
            if query:
                copy_query = f"COPY ({query}) TO STDOUT WITH CSV DELIMITER ';'"
                with open(file_path, 'w') as f_output:
                    db_cursor.copy_expert(copy_query, f_output)
    
            elif id_col_name:
                db_cursor.execute(f"""SELECT max({id_col_name}) FROM {table_name}""")
                row_num = db_cursor.fetchone()[0]
                print('Rows: ', row_num)
    
                row_min_pg = 0
                row_max_pg = row_num
    
                if update_type == 'increment':
                    n = get_last_id_bq(table_id, id_col_name)
                else:
                    n = 0
    
                if file_size_limit_rows:
                    if update_type != 'increment':
                        db_cursor.execute(f"""SELECT min({id_col_name}) FROM {table_name}""")
                        row_min_pg = db_cursor.fetchone()[0]
                        row_max_pg = row_min_pg + file_size_limit_rows
                        print('update_type: ', 'full')
                    else:
                        row_max_pg = n + file_size_limit_rows
                        print('update_type: ', 'increment')
                print('Rows start from: ', row_min_pg)
                print('Rows finish at: ', row_max_pg)
                ti.xcom_push(key='lower_line', value=n)
                ti.xcom_push(key='top_line', value=row_max_pg)
                while n < row_max_pg:
                    print('Start', n)
                    if n + batch <= row_max_pg:
                        n_to = n + batch
                    else:
                        n_to = row_max_pg
                    print('n_to ', n_to)
                    query = f'SELECT distinct * FROM {source_name} ' \
                            f'WHERE {id_col_name} >= {n} and {id_col_name} < {n_to}'
                    copy_query = f"COPY ({query}) TO STDOUT WITH CSV DELIMITER ';'"
                    with open(file_path, 'a') as f_output:
                        db_cursor.copy_expert(copy_query, f_output)
                    n += batch
            else:
                copy_query = f"COPY (SELECT distinct * FROM {source_name}) TO STDOUT WITH CSV DELIMITER ';'"
                with open(file_path, 'w') as f_output:
                    db_cursor.copy_expert(copy_query, f_output)
    
            conn.close()
        elif conn_type == 'clickhouse':
            host = conn_param.host
            user = conn_param.login
            password = conn_param.password
            database = conn_param.schema
            port = conn_param.port if conn_param.port else 9000
    
            client = clickhouse_driver.Client(host=host,
                                            database=database,
                                            user=user,
                                            password=password,
                                            port=port)
    
            if query:
                subp_res = subprocess.run(f'clickhouse-client --host {host} --user {user} --password {password} '
                                          f'--query "{query}" --format CSV > {file_path} ', shell=True,
                                          capture_output=True)
                logging.info(f"subprocess returncode: {subp_res.returncode}. stdout: {subp_res.stdout}")
                logging.error(f"subprocess stderr: {subp_res.stderr}")
                # os.system(f'clickhouse-client --host {host} --user {user} --password {password} --query "{query}"
                # --format CSV > {file_path}')
                #subprocess.check_output(f'clickhouse-client --host {host} --user {user} --password {password}
                # --query "{query}" --format CSV > {file_path}')
    
            elif id_col_name:
                if is_balval:
                    query = f"""SELECT max({id_col_name})+1
                                FROM (SELECT {id_col_name}, count(1) as n_row FROM {source_name} 
                                GROUP BY {id_col_name}) t
                                WHERE n_row >= (SELECT count(1) FROM {source_name} WHERE {id_col_name} = 
                                (SELECT max({id_col_name})-1 FROM {source_name}))"""
                    row_num = client.execute(query)[0][0]
                else:
                    row_num = client.execute(f"""SELECT max({id_col_name}) FROM {source_name}""")[0][0]
                print('Rows ', row_num)                
    
                if update_type == 'increment':
                    n = get_last_id_bq(table_id, id_col_name)
                    print(f'Max {id_col_name} in BQ: ', n)
                    # if n == row_num:
                    #     from airflow.exceptions import AirflowSkipException
                    #     AirflowSkipException('new rows not found')
                else:
                    n = 0
                ti.xcom_push(key='lower_line', value=n)
                ti.xcom_push(key='top_line', value=row_num)
                while n < row_num:
                    print('Start', n)
                    query = f'SELECT distinct {columns} FROM {source_name}{final} WHERE {id_col_name} >= {n} ' \
                            f'and {id_col_name} < {n+batch} and {id_col_name} < {row_num}'
                    print(query)
                    subp_res = subprocess.run(f'clickhouse-client --host {host} --user {user} --password {password} '
                                              f'--query "{query}" --format CSV >> {file_path}', shell=True,
                                              capture_output=True)
                    logging.info(f"subprocess returncode: {subp_res.returncode}. stdout: {subp_res.stdout}")
                    logging.error(f"subprocess stderr: {subp_res.stderr}")
                    # os.system(f'clickhouse-client --host {host} --user {user} --password {password}
                    # --query "{query}" --format CSV >> {file_path}')
                    n += batch
            else:
                subp_res = subprocess.run(f'clickhouse-client --host {host} --user {user} --password {password} '
                                          f'--query "SELECT distinct {columns} FROM {source_name}{final}" '
                                          f'--format CSV > {file_path}', shell=True, capture_output=True)
                logging.info(f"subprocess returncode: {subp_res.returncode}. stdout: {subp_res.stdout}")
                logging.error(f"subprocess stderr: {subp_res.stderr}")
                # os.system(f'clickhouse-client --host {host} --user {user} --password {password}
                # --query "SELECT {columns} FROM {source_name}{final}" --format CSV > {file_path}')

            subp_res = subprocess.run(f"sed -i 's/\\\\N//g' {file_path}", shell=True, capture_output=True)
            logging.info(f"subprocess returncode: {subp_res.returncode}. stdout: {subp_res.stdout}")
            logging.info(f"Clearing final: {table_name}")

        creds = Variable.get("sa_bigquery_token", deserialize_json=True)
        bucket = Variable.get("bigloader_bucket")
        
        storage_client = storage.Client.from_service_account_info(creds)
        bucket = storage_client.bucket(bucket)
        blob = bucket.blob(file_dir+file_name)
        print(file_name, file_path)
        blob.upload_from_filename(file_path)

def upload_data_to_bucket(**op_kwargs):
    file_name = op_kwargs['file_name'] if 'file_name' in op_kwargs else None
    file_path = op_kwargs['file_path'] if 'file_path' in op_kwargs else None

    creds = Variable.get("sa_bigquery_token", deserialize_json=True)
    bucket = Variable.get("bigloader_bucket")

    storage_client = storage.Client.from_service_account_info(creds)
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(file_name)
    print(file_name, file_path)
    blob.upload_from_filename(file_path)

def create_table_bq(**op_kwargs):
    data = op_kwargs['data'] if 'data' in op_kwargs else None
    prefix = op_kwargs['prefix'] if 'prefix' in op_kwargs else ''
    file_dir = op_kwargs['file_dir'] if 'file_dir' in op_kwargs else ''
    file_name = op_kwargs['file_name'] if 'file_name' in op_kwargs else None
    project_name = op_kwargs['project_name'] if 'project_name' in op_kwargs else Variable.get("airflow_env_params", deserialize_json=True)['bq_project_id']
    dataset_name = op_kwargs['dataset_name'] if 'dataset_name' in op_kwargs else None
    table_name = op_kwargs['table_name'] if 'table_name' in op_kwargs else None
    schema = op_kwargs['schema'] if 'schema' in op_kwargs else None
    field_delimiter = op_kwargs['field_delimiter'] if 'field_delimiter' in op_kwargs else ';'
    write_disposition = op_kwargs['write_disposition'] if 'write_disposition' in op_kwargs else None
    allow_jagged_rows = op_kwargs['allow_jagged_rows'] if 'allow_jagged_rows' in op_kwargs else False
    ignore_unknown_values = op_kwargs['ignore_unknown_values'] if 'ignore_unknown_values' in op_kwargs else True
    write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND if write_disposition == 'append' \
        else bigquery.job.WriteDisposition.WRITE_TRUNCATE
    schema_update_options = bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION \
        if write_disposition == bigquery.job.WriteDisposition.WRITE_APPEND else None
    load_table_from = op_kwargs['load_table_from'] if 'load_table_from' in op_kwargs else 'load_table_from_uri'
    skip_leading_rows = op_kwargs['skip_leading_rows'] if 'skip_leading_rows' in op_kwargs else 0
    max_bad_records = op_kwargs['max_bad_records'] if 'max_bad_records' in op_kwargs else 2
    allow_quoted_newlines = op_kwargs['allow_quoted_newlines'] if 'allow_quoted_newlines' in op_kwargs else False

    logical_date = op_kwargs['execution_date']
    logical_date_dir = logical_date.strftime("%Y%m%d")
    logical_date_subdir = logical_date.strftime("%H%M%S")
    file_dir = f'{file_dir}{logical_date_dir}/{logical_date_subdir}/' if len(file_dir)>0 else f'' 


    creds = Variable.get("sa_bigquery_token", deserialize_json=True)
    bucket = Variable.get("bigloader_bucket")
    
    client = bigquery.Client.from_service_account_info(creds)

    table_id = f"{project_name}.{dataset_name}.{prefix}{table_name}"

    if 'load' in load_table_from:
        if load_table_from == 'load_table_from_uri':
            job_config = bigquery.LoadJobConfig(schema=schema,
                                                  skip_leading_rows=skip_leading_rows,
                                                  source_format=bigquery.SourceFormat.CSV,
                                                  field_delimiter=field_delimiter,
                                                  ignore_unknown_values = ignore_unknown_values,
                                                  max_bad_records = max_bad_records,
                                                  # max_bad_records = 2,
                                                  write_disposition=write_disposition,
                                                  schema_update_options=schema_update_options,
                                                  allow_jagged_rows=allow_jagged_rows,
                                                  allow_quoted_newlines=allow_quoted_newlines)
            uri = f"gs://{bucket}/{file_dir}{file_name}"
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        elif load_table_from=='load_table_from_json':
            job_config = bigquery.LoadJobConfig(autodetect=False,
                                                write_disposition=write_disposition,
                                                schema=schema)
            load_job = client.load_table_from_json(destination=table_id,
                                                    project=client.project,
                                                    json_rows=data,
                                                    job_config=job_config)
        else:
            print('load_table_from:', load_table_from)
        load_job.result()
    else:
        if load_table_from == 'schema':
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
        else:
            print('load_table_from:', load_table_from)

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


def gather_dwh_delay_time(**op_kwargs):
    """this func is for blocks, eras, rounds only"""
    table_id = op_kwargs['table_id'] if 'table_id' in op_kwargs else None
    entity = op_kwargs['entity'] if 'entity' in op_kwargs else None
    schema = op_kwargs['schema'] if 'schema' in op_kwargs else 'stg_data'
    chain = op_kwargs['chain'] if 'chain' in op_kwargs else None
    prev_task_id = op_kwargs['prev_task_id'] if 'prev_task_id' in op_kwargs else 'error'
    ti = op_kwargs['ti']
    creds = Variable.get("sa_bigquery_token", deserialize_json=True)
    #bucket = Variable.get("bigloader_bucket")

    client = bigquery.Client.from_service_account_info(creds)
    try:
        lower_line = ti.xcom_pull(key='lower_line', task_ids=[prev_task_id])[0]
        top_line = ti.xcom_pull(key='top_line', task_ids=[prev_task_id])[0]
        print('lower_line: ', lower_line)
    except Exception as e:
        logging.error("XCOM error {e}".format(e=e))

    entity_name = entity[:-1]
    entity_id = entity[:-1] + '_id'

    sql = """{meta}
               INSERT INTO {schema}.mbelt3_{chain}__dwh_delay_time_ms
               select network_id,
                      '{entity_name}' as entity,
                      {entity_id},
                      'dwh_delay_time_ms' as name,
                      TIMESTAMP_DIFF(cast(current_datetime() as TIMESTAMP), row_time, MILLISECOND) as value,
                      row_id,
                      cast(cast(current_datetime() as TIMESTAMP) as TIMESTAMP) as row_time, 
               from {s} 
               where row_id>{lower_line} and row_id<={top_line}
               """.format(meta='/* {"app": "python_legacy", "owner": "ilia.malyshev@p2p.org"} */', schema=schema,
                          chain=chain, entity_name=entity_name, entity_id=entity_id, s=table_id,
                          lower_line=lower_line, top_line=top_line)
    client.query(sql)
    print(sql)


def clear_ch_balval_validators_summary(**op_kwargs):
    conn_id = op_kwargs['conn_id']
    ti = op_kwargs['ti']
    prev_task_id = op_kwargs['prev_task_id'] if 'prev_task_id' in op_kwargs else 'error'
    
    conn_param = BaseHook.get_connection(conn_id)
    conn_type = conn_param.conn_type

    host = conn_param.host
    user = conn_param.login
    password = conn_param.password
    database = conn_param.schema
    port = conn_param.port if conn_param.port else 9000

    client = clickhouse_driver.Client(host=host,
                                    database=database,
                                    user=user,
                                    password=password,
                                    port=port)

    try:
        id = int(ti.xcom_pull(key='top_line', task_ids=[prev_task_id])[0])-3500
        query = f"""ALTER TABLE {database}.validators_summary DELETE WHERE epoch <= {id}"""
        print(query)
        client.execute(query)
    except Exception as err:
        print(err)
        print('Error clear')
