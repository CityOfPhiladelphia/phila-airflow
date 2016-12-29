"""
ETL Pipeline for backing-up data sent to etl_dropbox

"""

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.operators import FolderDownloadOperator, FileAvailabilitySensor
from airflow.operators import SlackNotificationOperator
from datetime import datetime, timedelta

# =========================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    'on_failure_callback': SlackNotificationOperator.failed(),
}

pipeline = DAG('etl_backup_tax_delq',
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='@monthly',
    default_args=default_args
)

# make staging
mk_staging = CreateStagingFolder(task_id='staging', dag=pipeline)

# detect new file in directory
detect_file = FileAvailabilitySensor(task_id='detect_file',dag=pipeline,
        source_type='sftp',
        source_conn_id='phl-ftp-etl',
        source_path='/FakeStore/*',

        poke_interval=60*5, # 5 minutes,
        timeout=60*60*24*7
)

# extract file to be copied to staging
extract_data = FolderDownloadOperator(
        task_id='extract_data',
        dag=pipeline,

        source_type='sftp',
        source_conn_id='phl-ftp-etl',
        source_path='/FakeStore/',

        dest_path='{{ ti.xcom_pull("staging") }}'
)


# upload file
upload_to_archive  = BashOperator(
        task_id='upload',
        dag=pipeline,

        bash_command='gdrive upload --parent $REV {{ ti.xcom_pull("staging") }}/*'

)

cleanup_staging = DestroyStagingFolder(task_id='cleanup_staging', dag=pipeline,
        dir='{{ ti.xcom_pull("staging") }}',
)

mk_staging >> detect_file >> extract_data >> upload_to_archive >> cleanup_staging

