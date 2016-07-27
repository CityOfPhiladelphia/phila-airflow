from airflow import DAG
from airflow.operators import BranchPythonOperator
from airflow.models import Variable
from slackclient import SlackClient
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                   datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False, 'email_on_retry': False, 'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retries': 0
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'schedule_interval': timedelta(1),
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('notify', default_args=default_args)


print_date = BashOperator(
    task_id='print-date',
    xcom_push=True,
    bash_command= 'date ; echo $?',
    dag=dag
)

def process_status_code(**kwargs):
    ti = kwargs['ti']
    status_code = int(ti.xcom_pull('print_date'))
    if status_code  > 0:
        return 'notify_error'
    else:
        return 'notify_success'


# paths = [no_errors, errors]

branch = BranchPythonOperator(
    task_id='branch2',
    provide_context=True,
    python_callable=process_status_code,
    dag=dag
)

success = 'You should be happy Your Tasks finished without errors'

no_errors = SlackAPIPostOperator(
    task_id='notify_success',
    token=Variable.get('slack_token'),
    channel='C1SRU2R33',
    text=success,
    dag=dag
)


failure = 'Your Task(s) have  met with an error please try again :sadparrot:'

errors = SlackAPIPostOperator(
    task_id='notify_error',
    token=Variable.get('slack_token'),
    channel='C1SRU2R33',
    text=failure,
    dag=dag
)


dag.doc_md = "Slack Notifications"

print_date.set_downstream(branch)
branch.set_downstream(no_errors)
branch.set_downstream(errors)
