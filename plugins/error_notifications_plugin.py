"""
You can create a test Slack token at https://api.slack.com/docs/oauth-test-tokens
"""

import logging

from airflow.plugins_manager import AirflowPlugin

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators import SlackAPIPostOperator
from slackclient import SlackClient


class SlackNotificationOperator (SlackAPIPostOperator):
    @classmethod
    def failed(cls, slack_token_variable='slack_token', channel_name='pipeline-testing'):
        def wrapped(context):
            """ping error in slack on failure and provide link to the log"""
            conf = context["conf"]
            task = context["task"]
            execution_date = context["execution_date"]
            dag = context["dag"]
            base_url = conf.get('webserver', 'base_url')

            # Get the ID of the target slack channel
            slack_token = Variable.get(slack_token_variable)
            sc = SlackClient(slack_token)

            response = sc.api_call('channels.list')
            for channel in response['channels']:
                if channel['name'].lower() == channel_name.lower():
                    break
            else:
                raise AirflowException('No channel named {} found.'.format(channel_name))

            # Construct a slack operator to send the message off.
            notifier = cls(
                task_id='task_failed',
                token=slack_token,
                channel=channel['id'],
                text=(
                    "Your DAG has encountered an error, please follow the link "
                    "to view the log details:  "
                    "{}/admin/airflow/log?"
                        "task_id={}&"
                        "dag_id={}&"
                        "execution_date={}"
                    ).format(base_url, task.task_id, dag.dag_id,
                             execution_date.isoformat()),
                dag=dag,
            )
            notifier.execute()
        return wrapped


class ErrorNotificationPlugin(AirflowPlugin):
    name = "error_notification_plugin"
    operators = [SlackNotificationOperator]
