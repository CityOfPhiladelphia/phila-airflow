""" this is a docstring """
from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators import EmailOperator
from datetime import datetime, timedelta
import tempfile

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'schedule_interval': timedelta(1),
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('clean_opa2', default_args=default_args)

def mkdir():
	return tempfile.mkdtemp()


t1 = PythonOperator(
    task_id='temp_storage',
    python_callable=mkdir,
    dag=dag)



# t1, t2 and t3 are examples of tasks created by instantiating operators



t2 = BashOperator(
      task_id='run_scripts',
      bash_command='''cat ~/opa_data/br63trf.os13sd | phl-properties > properties.csv
  cat ~/opa_data/br63trf.buildcod | phl-building-codes > {{ ti.xcom_pull("temp_storage") }}/building_codes.csv
  cat ~/opa_data/br63trf.stcode | phl-street-codes > {{ ti.xcom_pull("temp_storage") }}/street_codes.csv
  cat ~/opa_data/br63trf.offpr | phl-off-property > {{ ti.xcom_pull("temp_storage") }}/off_property.csv
  cat ~/opa_data/br63trf.nicrt4wb | phl-assessment-history > {{ ti.xcom_pull("temp_storage") }}/assessment_history.csv''', dag=dag)

dag.doc_md = "hello"
t2.set_upstream(t1)
