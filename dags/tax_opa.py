"""
ETL Pipeline for OPA-Tax Data

"""

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import FileDownloadOperator
from airflow.operators import SlackNotificationOperator
from datetime import datetime, timedelta

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2017, 1, 1, 0, 0, 0),
    'on_failure_callback': SlackNotificationOperator.failed(),
}

pipeline_tax = DAG('etl_opa_tax_v1', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Transform - run each table through a cleanup script

def mkdir( ):
    import tempfile
    return tempfile.mkdtemp()

mk_staging = PythonOperator(
    task_id='staging',
    dag=pipeline_tax,

    python_callable=mkdir,
)

extract = FileDownloadOperator(
    task_id='download_property_tax',
    dag=pipeline_tax,
    source_type='sftp',
    source_path='/Test_RevenueRealEstate_Tax/sample.txt',
    dest_path='{{ ti.xcom_pull("staging")}}/sample.txt '
)

transform = BashOperator(
    task_id='',
    dag=pipeline_tax,

    bash_command='cat input/sample.txt | python main.py > output/sample.csv',

)

cleanup = CleanupOperator(
    task_id='cleanup_staging',
    dag=pipeline,
    paths='{{ ti.xcom_pull("staging") }}',
)

#==============================================================
# Configure the pipeline's dag

mk_staging >> extract >> transform >> cleanup
