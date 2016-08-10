import logging

from airflow.plugins_manager import AirflowPlugin

from airflow.exceptions import AirflowException
from airflow.operators import SlackAPIPostOperator

class SlackNotificationOperator (SlackAPIPostOperator):
    @classmethod
    def failed(cls, context):
        """ping error in slack on failure and provide link to the log"""
        conf = context["conf"]
        task = context["task"]
        execution_date = context["execution_date"]
        dag = context["dag"]
        base_url = conf.get('webserver', 'base_url')

        notifier = cls(
            task_id='task_failed',
            token=Variable.get('slack_token'),
            channel='C1SRU2R33',
            text=(
                "Your DAG has encountered an error, please follow the link to "
                "view the log details:  "
                "{}/admin/airflow/log?"
                    "task_id={}&"
                    "dag_id={}&"
                    "execution_date={}"
                ).format(base_url, task.task_id, dag.dag_id,
                         execution_date.isoformat()),
            dag=dag,
        )
        notifier.execute()


class ErrorNotificationPlugin(AirflowPlugin):
    name = "error_notification_plugin"
    operators = [SlackNotificationOperator]
