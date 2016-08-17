from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import DatumCSV2TableOperator
from airflow.operators import SlackNotifyOperator
from airflow.operators import FileTransferOperator
from airflow.models import Variable
from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False, 'email_on_retry': False, 'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retries': 0,
    'on_failure_callback': SlackNotifyOperator.failed,
}


dag = DAG('notify_plug', default_args=default_args)

print_date = BashOperator(
    task_id='print_date',
    xcom_push=True,
    bash_command= 'drumphagain',
    on_failure_callback=SlackNotifyOperator.failed,
    dag=dag
)
