"""
ETL Pipeline for OPA Data
"""

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import DatumLoadOperator
from airflow.operators import CleanupOperator
from airflow.operators import FolderDownloadOperator
from airflow.operators import SlackNotificationOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from datetime import datetime, timedelta

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.now().replace(hour=0, minute=0, second=0),
    'on_failure_callback': SlackNotificationOperator.failed(),
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

db_conn_id = 'phl-warehouse-staging'

pipeline = DAG('etl_opa_local_v2', default_args=default_args)

# ------------------------------------------------------------
# Extract - copy files to the staging area

mk_staging = CreateStagingFolder(
    task_id='staging',
    dag=pipeline,
)

extract = FolderDownloadOperator(
    task_id='download_properties',
    dag=pipeline,

    source_type='local',
    source_path='/home/mjumbewu/Programming/phila/property-assessments-data-pipeline/input',

    dest_path='{{ ti.xcom_pull("staging") }}/input',
)

# ------------------------------------------------------------
# Transform - run each table through a cleanup script

transform_a = BashOperator(
    task_id='clean_properties',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/input/\\\'br63trf.os13sd\\\' | '
        'phl-properties > {{ ti.xcom_pull("staging") }}/properties.csv',
)

transform_b = BashOperator(
    task_id='clean_building_codes',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/input/\\\'br63trf.buildcod\\\' | '
        'phl-building-codes > {{ ti.xcom_pull("staging") }}/building_codes.csv',
)

transform_c = BashOperator(
    task_id='clean_street_codes',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/input/\\\'br63trf.stcode\\\' | '
        'phl-street-codes > {{ ti.xcom_pull("staging") }}/street_codes.csv',
)

transform_d = BashOperator(
    task_id='clean_off_property',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/input/\\\'br63trf.offpr\\\' | '
        'phl-off-property > {{ ti.xcom_pull("staging") }}/off_property.csv',
)

transform_e = BashOperator(
    task_id='clean_assessment_history',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/input/\\\'br63trf.nicrt4wb\\\' | '
        'phl-assessment-history > {{ ti.xcom_pull("staging") }}/assessment_history.csv',
)


# ------------------------------------------------------------
# Load - copy tables into on-prem database(s)

load_a = DatumLoadOperator(
    task_id='load_properties',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/properties.csv',
    db_conn_id=db_conn_id,
    db_table_name='opa_properties',
)

load_b = DatumLoadOperator(
    task_id='load_building_codes',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/building_codes.csv',
    db_conn_id=db_conn_id,
    db_table_name='opa_building_codes',
)

load_c = DatumLoadOperator(
    task_id='load_street_codes',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/street_codes.csv',
    db_conn_id=db_conn_id,
    db_table_name='opa_street_codes',
)

load_d = DatumLoadOperator(
    task_id='load_off_property',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/off_property.csv',
    db_conn_id=db_conn_id,
    db_table_name='opa_off_property',
)

load_e = DatumLoadOperator(
    task_id='load_assessment_history',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/assessment_history.csv',
    db_conn_id=db_conn_id,
    db_table_name='opa_assessment_history',
)

# ------------------------------------------------------------
# Postscript - clean up the staging area

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("staging") }}',
)


# ============================================================
# Configure the pipeline's dag

mk_staging >> extract

extract >> transform_a >> load_a >> cleanup
extract >> transform_b >> load_b >> cleanup
extract >> transform_c >> load_c >> cleanup
extract >> transform_d >> load_d >> cleanup
extract >> transform_e >> load_e >> cleanup
