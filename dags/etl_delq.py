"""
ETL Pipeline for Tax Delinquency

Connections:

"""

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import FileDownloadOperator
from airflow.operators import DatumLoadOperator, DatumExecuteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.operators import SlackNotificationOperator
from datetime import datetime, timedelta

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    'on_failure_callback': SlackNotificationOperator.failed(),
}

pipeline_delq = DAG('etl_delq_v1',
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='@monthly',
    default_args=default_args
)

# ------------------------------------------------------------
# Extract - copy files to the staging area

mk_staging = CreateStagingFolder(task_id='staging', dag=pipeline_delq)

extract_delq = FileDownloadOperator(
    task_id='download_delq',
    dag=pipeline_delq,

    source_type='sftp',
    source_conn_id='phl-ftp-etl',
    source_path='/Revenue_RealEstate_Tax/sample.txt',

    dest_path='{{ ti.xcom_pull("staging") }}/sample.txt'
)

# -----------------------------------------------------
# Transform - run table through a cleanup script
# https://github.com/nwebz/property-tax-delinquency-pipeline

transform_delq = BashOperator(
    task_id='clean_delq',
    dag=pipeline_delq,

    bash_command='cat {{ ti.xcom_pull("staging") }}/sample.txt | delq_clean.py > {{ ti.xcom_pull("staging") }}/out.csv',

)

load_delq = DatumLoadOperator(
    task_id='load_delq',
    dag=pipeline_delq,

    csv_path='{{ ti.xcom_pull("staging") }}/out.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='tax_delq',
)

geocode_delq = DatumExecuteOperator(
    task_id='geocode_delq',
    dag=pipeline_delq,

    sql='''-- Refresh the public materialized view''',
    db_conn_id='phl-warehouse-staging',
)

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline_delq,
    dir='{{ ti.xcom_pull("staging") }}',
)

mk_staging >> extract_delq >> transform_delq >> load_delq >> geocode_delq >> cleanup
