"""
ETL Pipeline for Tax Delinquency

Connections:

"""

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import FileDownloadOperator
from airflow.operators import DatumLoadOperator
from airflow.operators import CleanupOperator
from airflow.operators import SlackNotificationOperator
from datetime import datetime, timedelta

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    # 'depends_on_past' False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2017, 1, 1, 0, 0, 0),
    'on_failure_callback': SlackNotificationOperator.failed(),
}

pipeline_delq = DAG('etl_delq_v1', default_args=default_args)

# ------------------------------------------------------------
# Extract - copy files to the staging area

mk_staging = CreateStagingFolder(task_id='staging', dag=pipeline_delq)

extract_delq = FileDownloadOperator(
    task_id='download_delq',
    dag=pipeline_delq,

    source_type='sftp',
    source_conn_id='phl-ftp-etl',
    source_path='/Test_Tax_Delinquents/sample.txt',

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

cleanup = CleanupOperator(
    task_id='cleanup_staging',
    dag=pipeline_delq,
    paths='{{ ti.xcom_pull("staging") }}',
)

mk_staging >> extract_delq >> transform_delq >> cleanup
