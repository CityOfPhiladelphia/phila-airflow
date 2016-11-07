"""
ETL Pipeline for Taxi Trip Data

Connections:
"""

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import DatumLoadOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.operators import FolderDownloadOperator, FileAvailabilitySensor
from airflow.operators import SlackNotificationOperator
from datetime import datetime, timedelta

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    'on_failure_callback': SlackNotificationOperator.failed(),
}

pipeline = DAG('etl_taxi_trips_v3',
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='@weekly',
    default_args=default_args
)

# Extract
# -------
# 1. Create a staging folder
# 2. Check twice per day to see whether the data has been uploaded
# 3. When the data is available, download into the staging folder

mk_staging = CreateStagingFolder(task_id='staging', dag=pipeline)

wait_for_verifone = FileAvailabilitySensor(task_id='wait_for_verifone', dag=pipeline,
    source_type='sftp',
    source_conn_id='phl-ftp-etl',
    source_path='/Taxi/verifone/*',
    poke_interval=43200,
)

wait_for_cmt = FileAvailabilitySensor(task_id='wait_for_cmt', dag=pipeline,
    source_type='sftp',
    source_conn_id='phl-ftp-etl',
    source_path='/Taxi/cmt/*',
    poke_interval=43200,
)

download_verifone = FolderDownloadOperator(task_id='download_verifone', dag=pipeline,
    source_type='sftp',
    source_conn_id='phl-ftp-etl',
    source_path='/Taxi/verifone',

    dest_path='{{ ti.xcom_pull("staging") }}/input/verifone',
)

download_cmt = FolderDownloadOperator(task_id='download_cmt', dag=pipeline,
    source_type='sftp',
    source_conn_id='phl-ftp-etl',
    source_path='/Taxi/cmt',

    dest_path='{{ ti.xcom_pull("staging") }}/input/cmt',
)

# Transform & Load
# ----------------
# 0. Merge all of the downloaded files into one CSV
# 1. Insert the merged data into an Oracle table.
# 2. Update the anonymization mapping tables.
# 3. Generalize ("fuzzy") the pickup and dropoff locations and times.
# 4. Insert the anonymized and fuzzied data into a public table.

normalize = BashOperator(task_id='merge_and_norm', dag=pipeline,
    bash_command=
        'taxitrips.py transform '
        '  --verifone "{{ ti.xcom_pull("staging") }}/input/verifone/*.csv"'
        '  --cmt "{{ ti.xcom_pull("staging") }}/input/cmt/*.csv" > '
        '{{ ti.xcom_pull("staging") }}/merged_trips.csv',
)

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

mk_staging  >>    wait_for_cmt     >>    download_cmt     >>  normalize
mk_staging  >>  wait_for_verifone  >>  download_verifone  >>  normalize

normalize  >>  load_raw  >>  anonymize
normalize  >>   fuzzy    >>  anonymize

anonymize  >>  load_public  >>  cleanup
