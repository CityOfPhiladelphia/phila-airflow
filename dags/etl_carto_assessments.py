from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators import TheELOperator
from airflow.operators import SlackNotificationOperator

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    'on_failure_callback': SlackNotificationOperator.failed(),
}

dag = DAG('etl_carto_assessments',
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='0 2 * * *',
    default_args=default_args
)

schema_file = 's3://"$S3_SCHEMA_BUCKET"/opa_assessments.json'
data_file = 's3://"$S3_STAGING_BUCKET"/etl_carto_assessments/{{run_id}}/etl_carto_assessments.csv'

extract_assessments = TheELOperator(
    task_id='extract_assessments',
    dag=dag,
    el_command='read',
    db_schema='GIS_OPA',
    table_name='assessments',
    connection_string='"$GEODB2_CONN_STRING"',
    output_file=data_file
)

create_temp_assessments_table = TheELOperator(
    task_id='create_temp_assessments_table',
    dag=dag,
    el_command='create_table',
    db_schema='phl',
    table_name='assessments_{{run_id}}',
    table_schema_path=schema_file,
    connection_string='"$CARTO_CONN_STRING"'
)

load_assessments = TheELOperator(
    task_id='load_assessments',
    dag=dag,
    el_command='write',
    db_schema='phl',
    table_name='assessments_{{run_id}}',
    table_schema_path=schema_file,
    connection_string='"$CARTO_CONN_STRING"',
    input_file=data_file
)

swap_assessments = TheELOperator(
    task_id='swap_assessments',
    dag=dag,
    el_command='swap_table',
    db_schema='phl',
    new_table_name='assessments_{{run_id}}',
    old_table_name='awm_assessments',
    connection_string='"$CARTO_CONN_STRING"'
)

extract_assessments >> create_temp_assessments_table >> load_assessments >> swap_assessments
