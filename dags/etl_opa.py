"""
ETL Pipeline for OPA Data

Connections:
- phl-s3-etl (must have credentials set in a JSON object in the Extra field,
  i.e.: {"aws_access_key_id":"...", "aws_secret_access_key":"..."})
"""

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import DatumCSV2TableOperator
from airflow.operators import FileTransferOperator
from datetime import datetime, timedelta

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(1, 1, 1, 0, 0, 0),
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_opa', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Extract - copy files to the staging area

def mkdir():
    import tempfile
    return tempfile.mkdtemp()

t0 = PythonOperator(
    task_id='staging',
    dag=pipeline,

    python_callable=mkdir,
)

t1a = FileTransferOperator(
    task_id='download_properties',
    dag=pipeline,

    source_type='local',
    source_path='/home/mjumbewu/Programming/phila/property-assessments-data-pipeline/input/\'br63trf.os13sd\'',

    dest_type='local',
    dest_path='{{ ti.xcom_pull("staging") }}/br63trf.os13sd',
)

t1b = FileTransferOperator(
    task_id='download_building_codes',
    dag=pipeline,

    source_type='local',
    source_path='/home/mjumbewu/Programming/phila/property-assessments-data-pipeline/input/\'br63trf.buildcod\'',

    dest_type='local',
    dest_path='{{ ti.xcom_pull("staging") }}/br63trf.buildcod',
)

t1c = FileTransferOperator(
    task_id='download_street_codes',
    dag=pipeline,

    source_type='local',
    source_path='/home/mjumbewu/Programming/phila/property-assessments-data-pipeline/input/\'br63trf.stcode\'',

    dest_type='local',
    dest_path='{{ ti.xcom_pull("staging") }}/br63trf.stcode',
)

t1d = FileTransferOperator(
    task_id='download_off_property',
    dag=pipeline,

    source_type='local',
    source_path='/home/mjumbewu/Programming/phila/property-assessments-data-pipeline/input/\'br63trf.offpr\'',

    dest_type='local',
    dest_path='{{ ti.xcom_pull("staging") }}/br63trf.offpr',
)

t1e = FileTransferOperator(
    task_id='download_assessment_history',
    dag=pipeline,

    source_type='local',
    source_path='/home/mjumbewu/Programming/phila/property-assessments-data-pipeline/input/\'br63trf.nicrt4wb\'',

    dest_type='local',
    dest_path='{{ ti.xcom_pull("staging") }}/br63trf.nicrt4wb',
)

# ------------------------------------------------------------
# Transform - run each table through a cleanup script

t2a = BashOperator(
    task_id='clean_properties',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/br63trf.os13sd | '
        'phl-properties > {{ ti.xcom_pull("staging") }}/properties.csv',
)

t2b = BashOperator(
    task_id='clean_building_codes',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/br63trf.buildcod | '
        'phl-building-codes > {{ ti.xcom_pull("staging") }}/building_codes.csv',
)

t2c = BashOperator(
    task_id='clean_street_codes',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/br63trf.buildcod | '
        'phl-street-codes > {{ ti.xcom_pull("staging") }}/building_codes.csv',
)

t2d = BashOperator(
    task_id='clean_off_property',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/br63trf.offpr | '
        'phl-off-property > {{ ti.xcom_pull("staging") }}/off_property.csv',
)

t2e = BashOperator(
    task_id='clean_assessment_history',
    dag=pipeline,

    bash_command=
        'cat {{ ti.xcom_pull("staging") }}/br63trf.nicrt4wb | '
        'phl-assessment-history > {{ ti.xcom_pull("staging") }}/assessment_history.csv',
)


# ------------------------------------------------------------
# Load - copy tables into on-prem database(s)

t3a = DatumCSV2TableOperator(
    task_id='load_properties',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/properties.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='opa_properties',
)

t3b = DatumCSV2TableOperator(
    task_id='load_building_codes',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/building_codes.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='opa_building_codes',
)

t3c = DatumCSV2TableOperator(
    task_id='load_street_codes',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/street_codes.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='opa_street_codes',
)

t3d = DatumCSV2TableOperator(
    task_id='load_off_property',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/off_property.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='opa_off_property',
)

t3e = DatumCSV2TableOperator(
    task_id='load_assessment_history',
    dag=pipeline,

    csv_path='{{ ti.xcom_pull("staging") }}/assessment_history.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='opa_assessment_history',
)

# ------------------------------------------------------------
# Postscript - clean up the staging area

# t3 = S3CleanupOperator(
#     task_id='cleanup_staging',
#
#     s3_conn_id='phl-s3-etl',
#     s3_key='s3://airflow-staging/opa/{{ ts_nodash }}'
# )


# ============================================================
# Configure the pipeline's dag

t0 >> t1a >> t2a >> t3a
t0 >> t1b >> t2b >> t3b
t0 >> t1c >> t2c >> t3c
t0 >> t1d >> t2d >> t3d
t0 >> t1e >> t2e >> t3e
