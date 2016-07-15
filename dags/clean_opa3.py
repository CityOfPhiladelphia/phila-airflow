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
    'email_on_failure': True, 'email_on_retry': False, 'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'schedule_interval': timedelta(1),
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('clean_opa3', default_args=default_args)

def mkdir():
	return tempfile.mkdtemp()


t1 = PythonOperator(
    task_id='temp_storage',
    python_callable=mkdir,
    dag=dag)


t2 = BashOperator(
      task_id='clean_properties',
      bash_command='''cat ~/opa_data/br63trf.os13sd | phl-properties > {{ ti.xcom_pull("temp_storage") }}/properties.csv''', dag=dag)


t3 = BashOperator(
        task_id='clean_buildings',
        bash_command='''cat ~/opa_data/br63trf.buildcod | phl-building-codes > {{ ti.xcom_pull("temp_storage") }}/building_codes.csv''', dag=dag)

t4 = BashOperator(
        task_id='clean_street_codes',
        bash_command='''cat ~/opa_data/br63trf.stcode | phl-street-codes > {{ ti.xcom_pull("temp_storage") }}/street_codes.csv''', dag=dag)

t5 = BashOperator(
        task_id='clean_off_property',
        bash_command='''cat ~/opa_data/br63trf.offpr | phl-off-property > {{ ti.xcom_pull("temp_storage") }}/off_property.csv''', dag=dag)

t6 = BashOperator(
        task_id='clean_assessment_history',
        bash_command='''cat ~/opa_data/br63trf.nicrt4wb | phl-assessment-history > {{ ti.xcom_pull("temp_storage") }}/assessment_history.csv''', dag=dag)


t7 = BashOperator(
        task_id='clear_temp',
        bash_command='''rm -rf {{ ti.xcom_pull("temp_storage") }} ''', dag=dag)

dag.doc_md = "Scrub OPA Task Runner"
t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t1)
t5.set_upstream(t1)
t6.set_upstream(t1)

t2.set_downstream(t7)
t3.set_downstream(t7)
t4.set_downstream(t7)
t5.set_downstream(t7)
t6.set_downstream(t7)
