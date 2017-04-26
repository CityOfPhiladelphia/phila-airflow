from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators import TheELOperator
from airflow.operators import SlackNotificationOperator

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    'on_failure_callback': SlackNotificationOperator.failed(),
}

dag = DAG('etl_bev_tax_accounts_v1',
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='@hourly',
    default_args=default_args
)
## TODO: !!! add age out on the staging buckets
## TODO: add datetime / run id to staging files?
schema_file = 's3://"$S3_STAGING_BUCKET"/schemas/etl_bev_tax_accounts.json'
data_file = 's3://"$S3_STAGING_BUCKET"/data/etl_bev_tax_accounts.csv'

extract_accounts_schema = TheELOperator(
    task_id='extract_accounts_schema',
    dag=dag,
    el_command='describe_table',
    table_name='VW_AccountDetails',
    connection_string='"$BEV_TAX_MSSQL_CONN_STRING"',
    output_file=schema_file
)

extract_accounts_data = TheELOperator(
    task_id='extract_accounts_data',
    dag=dag,
    el_command='read',
    table_name='VW_AccountDetails',
    connection_string='"$BEV_TAX_MSSQL_CONN_STRING"',
    output_file=data_file
)

