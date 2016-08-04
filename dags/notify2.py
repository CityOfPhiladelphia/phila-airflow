from airflow import DAG
from airflow.operators import BranchPythonOperator
from airflow.models import Variable
from slackclient import SlackClient
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


seven_days_ago = datetime.combine(datetime.today() - timedelta(9), datetime.min.time())


def failed(context):
    """ping error in slack on failure and provide link to the log"""
    conf = context["conf"]
    task = context["task"]
    execution_date = context["execution_date"]
    errors = SlackAPIPostOperator(
        task_id='task_failed',
        token=Variable.get('slack_token'),
        channel='C1SRU2R33',
        text="http://localhost:8080/admin/airflow/log?" + "task_id=" + task.task_id + "&" +\
        "execution_date=" + execution_date.isoformat() + "&" + "dag_id=notify2",
        dag=dag
    )

    errors.execute()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False, 'email_on_retry': False, 'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retries': 0,
    'on_failure_callback': failed
}

dag = DAG('notify2', default_args=default_args)



print_date = BashOperator(
    task_id='print_date',
    xcom_push=True,
    bash_command= 'drumphagain',
    dag=dag
)


dag.doc_md = "Slack Notifications"



