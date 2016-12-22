from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import EmailOperator
from airflow.operators import S3KeySensor
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

dag = DAG('basic', default_args=default_args)
# dag.doc_md = __doc__

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='echo',
    bash_command='echo this is some sad stuff > sad_stuff.txt',
    dag=dag)



t2 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command='date',
    dag=dag)
# t2.doc_md = """\
# #Title"
# Wiki[Airflow Wikipage](https://github.com/CityOfPhiladelphia/oddt-wiki/wiki/Airflow-Server-Configurations)
# """


t1 >> t2
