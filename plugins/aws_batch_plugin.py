import logging

import boto3

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.models import 
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session

class AWSBatchExecutor(BaseExecutor):
    """
    Executes tasks on AWS Batch.

    Assumes a compute environment, job queue, and job definition have been created.
    """

    def start(self):
        # self.batch_client = boto3.client('batch')
        # self.default_job_queue = configuration.get('aws_batch', 'DEFAULT_JOB_QUEUE')
        # self.default_job_definition = configuration.get('aws_batch', 'DEFAULT_JOB_DEFINITION')
        print('AWSBatchPlugin - start')

    def sync(self):
        #self.batch_client.list_jobs(jobQueue='string')
        ##          - paginate
        ##          - find queue names from self.running
        ##          - set self.change_state() for each task
        print('AWSBatchPlugin - sync')

    def execute_async(self, key, command, queue=None):
        #command, priority, queue, task_instance = self.queued_tasks[key] ## TODO: !!! popped?

        print('AWSBatchPlugin - execute_async')
        print(self.running)
        print(key)
        print(self.running[key])
        print(command)
        print(queue)

        # if 'job_definition' in task_instance.task.params:
        #     job_definition = task_instance.task.params
        # else:
        #     job_definition = self.default_job_definition

        # self.batch_client.submit_job(
        #     jobName=key,
        #     jobQueue=queue or self.default_job_queue,
        #     jobDefinition=job_definition,
        #     paramaters={
        #         'dag_id': task_instance.dag_id,
        #         'task_id': task_instance.task_id,
        #         'start_date': task_instance.start_date
        #     },
        #     containerOverrides={
        #         'environment': [
        #             {
        #                 'name': 'AIRFLOW_DAG_ID',
        #                 'value': task_instance.dag_id
        #             },
        #             {
        #                 'name': 'AIRFLOW_TASK_ID',
        #                 'value': task_instance.task_id
        #             },
        #             {
        #                 'name': 'AIRFLOW_START_DATE',
        #                 'value': task_instance.start_date
        #             }
        #         ]
        #     })

    def end(self):
        print('AWSBatchPlugin - end')

    def terminate(self):
        print('AWSBatchPlugin - terminate')

class AWSBatchPlugin(AirflowPlugin):
    name = "aws_batch_plugin"
    executors = [AWSBatchExecutor]
