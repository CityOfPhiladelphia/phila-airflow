"""
ETL Pipeline for OPA-Tax Data

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

pipeline_tax = DAG('etl_tax_v1', default_args=default_args)

# ------------------------------------------------------------
# Extract - copy files to the staging area

def mkdir( ):
    import tempfile
    return tempfile.mkdtemp()

mk_staging = PythonOperator(
    task_id='staging',
    dag=pipeline_tax,

    python_callable=mkdir,
)

extract = FileDownloadOperator(
    task_id='download_tax',
    dag=pipeline_tax,

    source_type='sftp',
    source_conn_id='phl-ftp-etl',
    source_path='/Revenue_RealEstate_Tax/sample.txt',

    dest_path='{{ ti.xcom_pull("staging") }}/sample.txt'
)

# -----------------------------------------------------
# Transform - run table through a cleanup script
# https://github.com/nwebz/property-tax-data-pipeline

transform = BashOperator(
    task_id='clean_tax',
    dag=pipeline_tax,

    bash_command='cat {{ ti.xcom_pull("staging") }}/sample.txt | tax_clean.py > {{ ti.xcom_pull("staging") }}/processed_tax.csv',

)

cleanup = CleanupOperator(
    task_id='cleanup_staging',
    dag=pipeline_tax,
    paths='{{ ti.xcom_pull("staging") }}',
)

#==============================================================
# Load

load = DatumLoadOperator(
    task_id='load_clean_tax',
    dag=pipeline_tax,

    csv_path='{{ ti.xcom_pull("staging") }}/processed_tax.csv',
    db_conn_id='phl-warehouse-staging',
    db_table_name='cleaned_tax',

)


#==============================================================
# Configure the pipeline's dag

mk_staging >> extract >> transform >> load >> cleanup
