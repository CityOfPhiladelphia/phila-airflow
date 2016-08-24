from airflow.plugins_manager import AirflowPlugin
from airflow.hooks import BaseHook
from airflow.models import BaseOperator
from airflow.models import Variable
from slackclient import SlackClient
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

class SlackNotifyOperator(BaseOperator):
    """
    Pings a specified channel with a link to a log error when a given task fails

    """
    @staticmethod
    def failed(self, context):
        self.conf = context["conf"]
        self.task = context["task"]
        self.execution_date = context["execution_date"]
        self.dag = context["dag"]
        self.errors = SlackAPIPostOperator(
            task_id='task_failed',
            token=Variable.get('slack_token'),
            channel='C1SRU2R33',
            text="Your DAG has encountered an error, please follow the link to view the log details:  " + "http://localhost:8080/admin/airflow/log?" + "task_id=" + task.task_id + "&" +\
            "execution_date=" + execution_date.isoformat() + "&" + "dag_id=" + dag.dag_id,
            dag=pipeline
        )

        errors.execute()



# ===============================================================
# Plugin Definition

class SlackNotifyPlugin(AirflowPlugin):
    name = "slack_notify_plugin"
    operators = [SlackNotifyOperator]
