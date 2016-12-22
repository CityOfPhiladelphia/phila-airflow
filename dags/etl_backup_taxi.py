"""
ETL Pipeline for backing-up PPA taxi-trips data sent to etl_dropbox

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

pipeline = DAG('etl_backup_taxi',
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='@monthly',
    default_args=default_args
)

# make staging
mk_staging = CreateStagingFolder(task_id='staging', dag=pipeline)

# wait for cmt
wait_for_cmt = FileAvailabilitySensor(task_id='wait_for_cmt',dag=pipeline,
        source_type='sftp',
        source_conn_id='phl-ftp-etl',
        source_path='/Taxi/cmt/*',

        poke_interval=60*5, # 5 minutes,
        timeout=60*60*24*7
)

# wait for verifone
wait_for_verifone = FileAvailabilitySensor(task_id='wait_for_verifone',dag=pipeline,
        source_type='sftp',
        source_conn_id='phl-ftp-etl',
        source_path='/Taxi/verifone/*',

        poke_interval=60*5, # 5 minutes,
        timeout=60*60*24*7
)

# download cmt data
extract_cmt = FolderDownloadOperator(
        task_id='download_cmt',
        dag=pipeline,

        source_type='sftp',
        source_conn_id='phl-ftp-etl',
        source_path='/Taxi/cmt/',

        dest_path='{{ ti.xcom_pull("staging") }}/input/cmt',
)

# download verifone data
extract_verifone = FolderDownloadOperator(
        task_id='download_verifone',
        dag=pipeline,

        source_type='sftp',
        source_conn_id='phl-ftp-etl',
        source_path='/Taxi/verifone/',

        dest_path='{{ ti.xcom_pull("staging") }}/input/verfone',
)

# upload cmt
upload_cmt = BashOperator(
        task_id='upload_cmt',
        dag=pipeline,

        bash_command='gdrive upload {{ ti.xcom_pull("staging") }}/input/cmt/*'

)

# upload verifone
upload_verifone = BashOperator(
    task_id='upload_verifone',
    dag=pipeline,

    bash_command='gdrive upload {{ ti.xcom_pull("staging") }}/input/verifone/*'
)

# delete staging folder
cleanup_staging = DestroyStagingFolder(task_id='cleanup_staging', dag=pipeline,
    dir='{{ ti.xcom_pull("staging") }}',
)


mk_staging >> wait_for_cmt >> extract_cmt >> upload_cmt
mk_staging >> wait_for_verifone >> extract_verifone >> upload_verifone >> cleanup_staging

