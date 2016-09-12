"""
ETL Pipeline for Taxi Trip Data

Connections:
"""

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import DatumLoadOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.operators import FolderDownloadOperator
from airflow.operators import SlackNotificationOperator
from datetime import datetime, timedelta

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2017, 1, 1, 0, 0, 0),
    'on_failure_callback': SlackNotificationOperator.failed(),
}

pipeline = DAG('etl_taxi_trips_v1', default_args=default_args)

# Extract - create a staging folder and copy there
mk_staging = CreateStagingFolder(task_id='staging', dag=pipeline)

extract = FolderDownloadOperator(task_id='download_properties', dag=pipeline,
    source_type='sftp',
    source_conn_id='phl-ftp-etl',
    source_path='/Taxi',

    dest_path='{{ ti.xcom_pull("staging") }}/input',
)

# Transform - merge all of the downloaded files into one CSV
transform = BashOperator(task_id='merge_trips', dag=pipeline,
    bash_command=
        'taxitrips.py transform '
        '  -v "{{ ti.xcom_pull("staging") }}/*.csv"'
        '  -c "{{ ti.xcom_pull("staging") }}/*.csv" > '
        '{{ ti.xcom_pull("staging") }}/merged_trips.csv',
)

# Load - Insert the merged data into an Oracle table; update the anonymization
#        mapping.
load = DatumLoadOperator(task_id='load_properties', dag=pipeline,
    csv_path='{{ ti.xcom_pull("staging") }}/merged_trips.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='taxi_trips',
)

cleanup = DestroyStagingFolder(task_id='cleanup_staging', dag=pipeline,
    dir='{{ ti.xcom_pull("staging") }}',
)


# ============================================================
# Configure the pipeline's dag

mk_staging >> extract >> transform >> load >> cleanup
