from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators import TheELOperator
from airflow.operators import SlackNotificationOperator

default_args = {
    'owner': 'airflow',
    'on_failure_callback': SlackNotificationOperator.failed(),
}

dag = DAG('etl_bev_tax_accounts_v1',
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='@hourly',
    default_args=default_args
)

schema_file = 's3://"$S3_STAGING_BUCKET"/schemas/etl_bev_tax_accounts.json'
data_file = 's3://"$S3_STAGING_BUCKET"/etl_bev_tax_accounts_v1/{{run_id}}/etl_bev_tax_accounts.csv'

extract_accounts_data = TheELOperator(
    task_id='extract_accounts_data',
    dag=dag,
    el_command='read',
    table_name='VW_AccountDetails',
    connection_string='"$BEV_TAX_MSSQL_CONN_STRING"',
    output_file=data_file
)

create_temp_table_accounts_data = TheELOperator(
    task_id='create_temp_table_accounts_data',
    dag=dag,
    el_command='create_table',
    db_schema='phl',
    table_name='sbt_accounts_{{run_id.lower()}}',
    table_schema_path=schema_file,
    connection_string='"$CARTO_CONN_STRING"'
)

load_accounts_data = TheELOperator(
    task_id='load_accounts_data',
    dag=dag,
    el_command='write',
    db_schema='phl',
    table_name='sbt_accounts_{{run_id.lower()}}',
    skip_headers=True,
    table_schema_path=schema_file,
    connection_string='"$CARTO_CONN_STRING"',
    input_file=data_file
)

swap_accounts_data = TheELOperator(
    task_id='swap_accounts_data',
    dag=dag,
    el_command='swap_table',
    db_schema='phl',
    new_table_name='sbt_accounts_{{run_id.lower()}}',
    old_table_name='sbt_accounts',
    connection_string='"$CARTO_CONN_STRING"'
)

extract_accounts_data >> create_temp_table_accounts_data >> load_accounts_data >> swap_accounts_data
