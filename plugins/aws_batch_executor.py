import logging

import boto3

from airflow import configuration
from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor

class AWSBatchExecutor(BaseExecutor):
    """
    """

    def start(self):
        self.batch_client = boto3.client('batch')
        self.default_queue = configuration.get('aws_batch', 'DEFAULT_QUEUE')
        self.default_definition = configuration.get('aws_batch', 'DEFAULT_DEFINITION')
        ## TODO: upsert compute environment? - so far this should be passed in config
        ## TODO: upsert job queue(s)? - if exists, do not create / update?
        ## TODO: upsert job definition? - if exists, do not create / update?
        pass

    def sync(self):
        ## TODO: client.list_jobs(jobQueue='string')
        ##          - paginate
        ##          - find queue names from self.running
        ##          - set self.change_state() for each task
        pass

    def execute_async(self, key, command, queue=None):
        command, priority, queue, task_instance = self.queued_tasks[key]

        ## TODO: ignore priority?

        if 'job_definition' in task_instance.task.params:
            job_definition = task_instance.task.params
        else:
            job_definition = self.default_definition

        self.batch_client.submit_job(
            jobName=key,
            jobQueue=queue or self.default_queue,
            jobDefinition=job_definition,
            paramaters={
                'dag_id': task_instance.dag_id,
                'task_id': task_instance.task_id,
                'start_date': task_instance.start_date
            },
            containerOverrides={
                'environment': [
                    {
                        'name': 'AIRFLOW_DAG_ID',
                        'value': task_instance.dag_id
                    },
                    {
                        'name': 'AIRFLOW_TASK_ID',
                        'value': task_instance.task_id
                    },
                    {
                        'name': 'AIRFLOW_START_DATE',
                        'value': task_instance.start_date
                    }
                ]
            })

class AWSBatchPlugin(AirflowPlugin):
    name = "aws_batch_plugin"
    executors = [AWSBatchExecutor]
