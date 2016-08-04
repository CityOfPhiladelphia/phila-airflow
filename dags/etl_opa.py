"""
ETL Pipeline for OPA Data

Connections:
- phl-s3-etl (must have credentials set in a JSON object in the Extra field,
  i.e.: {"aws_access_key_id":"...", "aws_secret_access_key":"..."})
"""

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import FileTransferOperator
from airflow.operators import FileTransformOperator
from datetime import datetime, timedelta
import tempfile

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

# ------------------------------------------------------------
# Extract - copy files to the staging S3 bucket

t0 = FileTransferOperator(
    task_id='download_property_input',

    source_type='local',
    source_path='/home/mjumbewu/Programming/phila/property-assessments-data-pipeline/input/\'br63trf.os13sd\'',

    dest_type='s3',
    dest_conn_id='phl-s3-etl',
    dest_path='s3://airflow-staging/opa/{{ ts_nodash }}/input/br63trf.os13sd',

    **default_args,
)

# ------------------------------------------------------------
# Transform - generate tables suitable for loading into DB

t1 = FileTransformOperator(
    task_id='transform_property_input',

    source_type='s3',
    source_conn_id='phl-s3-etl',
    source_path='s3://airflow-staging/opa/{{ ts_nodash }}/input/br63trf.os13sd',

    dest_type='s3',
    dest_conn_id='phl-s3-etl',
    dest_path='s3://airflow-staging/opa/{{ ts_nodash }}/output/properties.csv',

    transform_script='phl-properties',

    **default_args,
)

# ------------------------------------------------------------
# Load - copy tables into on-prem database(s)

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

pipeline = DAG('etl_opa', default_args=default_args)  # TODO: Look up how to schedule a DAG

pipeline >> t0 >> t1
