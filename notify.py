from airflow import DAG
from airflow.operators import BranchPythonOperator
from airflow.models import Variable
from slackclient import SlackClient
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.bash_operator import BashOperator
from datetime import *


seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                   datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False, 'email_on_retry': False, 'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'schedule_interval': timedelta(1),
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('notify', default_args=default_args)


print_date = BashOperator(
    task_id='print_date',
    bash_command='date | echo $?',
    dag=dag
)

# paths = [no_errors, errors]

branch = BranchPythonOperator(
    task_id='branch',
    #compare value of exit code sc > 0 return failure
    # not sure how to write this branch i was looking at an example at:
    # https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/example_branch_operator.py

    python_callable=abs():
    if abs(sc) > 0:
        return notify_error,
    dag=dag
)

success = 'Your Task(s) finished without errors :pp-conga:'

no_errors = SlackAPIPostOperator(
    task_id='notify_success',
    token=Variable.get('slack_token'),
    channel='C1SRU2R33',
    text=success,
    dag=dag
)

no_errors.execute()

failure = 'Your Task(s) have  met with an error please try again :sadparrot:'

errors = SlackAPIPostOperator(
    task_id='notify_error',
    token=Variable.get('slack_token'),
    channel='C1SRU2R33',
    text=failure,
    dag=dag
)

errors.execute()

dag.doc_md = "Slack Notifications"

no_errors.set_upstream(print_date)
