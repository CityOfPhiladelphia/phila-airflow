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

pipeline = DAG('etl_taxi_trips_v2', default_args=default_args)

# Extract - create a staging folder and copy there
mk_staging = CreateStagingFolder(task_id='staging', dag=pipeline)

extract = FolderDownloadOperator(task_id='download', dag=pipeline,
    source_type='sftp',
    source_conn_id='phl-ftp-etl',
    source_path='/Taxi',

    dest_path='{{ ti.xcom_pull("staging") }}/input',
)

# Transform - merge all of the downloaded files into one CSV
normalize = BashOperator(task_id='merge_and_norm', dag=pipeline,
    bash_command=
        'taxitrips.py transform '
        '  --verifone "{{ ti.xcom_pull("staging") }}/input/verifone/*.csv"'
        '  --cmt "{{ ti.xcom_pull("staging") }}/input/cmt/*.csv" > '
        '{{ ti.xcom_pull("staging") }}/merged_trips.csv',
)

# Load - Insert the merged data into an Oracle table; update the anonymization
#        mapping.
load_raw = DatumLoadOperator(task_id='load_raw', dag=pipeline,
    csv_path='{{ ti.xcom_pull("staging") }}/merged_trips.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='taxi_trips',
)

fuzzy = BashOperator(task_id='fuzzy_time_and_loc', dag=pipeline,
    bash_command=
        'taxitrips.py transform '
        '  --verifone "{{ ti.xcom_pull("staging") }}/input/verifone/*.csv"'
        '  --cmt "{{ ti.xcom_pull("staging") }}/input/cmt/*.csv" > '
        '{{ ti.xcom_pull("staging") }}/merged_trips.csv',
)

anonymize = BashOperator(task_id='anonymize', dag=pipeline,
    bash_command=
        'taxitrips.py anonymize '
        '  "{{ ti.xcom_pull("staging") }}/fuzzied_trips.csv"'
        '  --cmt "{{ ti.xcom_pull("staging") }}/input/cmt/*.csv" > '
        '{{ ti.xcom_pull("staging") }}/merged_trips.csv',
)

load_public = DatumLoadOperator(task_id='load_public', dag=pipeline,
    csv_path='{{ ti.xcom_pull("staging") }}/merged_trips.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='taxi_trips',
)

cleanup = DestroyStagingFolder(task_id='cleanup_staging', dag=pipeline,
    dir='{{ ti.xcom_pull("staging") }}',
)


# ============================================================
# Configure the pipeline's dag

mk_staging >> extract >> normalize

normalize >> load_raw >> anonymize
normalize >> fuzzy >> anonymize

anonymize >> load_public >> cleanup
