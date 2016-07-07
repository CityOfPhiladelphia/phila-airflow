from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import EmailOperator
from datetime import datetime, timedelta

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

dag = DAG('upgrade', default_args=default_args)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='update',
    bash_command='sudo apt-get update',
    dag=dag)


#dag.doc_md = __doc__

t2 = BashOperator(
    task_id='echo',
    depends_on_past=False,
    bash_command='echo "update complete" ',
    dag=dag)



t2.set_upstream(t1)
