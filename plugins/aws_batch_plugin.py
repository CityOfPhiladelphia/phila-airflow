import logging
from datetime import datetime

import boto3

from airflow import configuration, models, settings
from airflow.executors.base_executor import BaseExecutor
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session
from airflow.utils.state import State

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

class AWSBatchExecutor(BaseExecutor):
    """
    Executes tasks on AWS Batch.

    Assumes a compute environment, job queue, and job definition have been created.
    """

    def __init__(self, *args, **kwargs):
        self.tasks = {}
        self.last_state = {}
        super(AWSBatchExecutor, self).__init__(*args, **kwargs)

    def get_jobs_with_status(self, status, next_token=None):
        params = {
            'jobQueue': self.job_queue,
            'jobStatus': status
        }

        if next_token:
            params['nextToken'] = next_token

        response = self.batch_client.list_jobs(**params)

        if len(response['jobSummaryList']) == 0:
            return

        response = self.batch_client.describe_jobs(
            jobs=list(map(lambda x: x['jobId'], response['jobSummaryList']))
        )

        for job in response['jobs']:
            yield job

        if 'nextToken' in response and response['nextToken']:
            yield from self.get_jobs_with_status(status, next_token=response['nextToken'])

    def get_jobs(self, next_token=None):
        for status in ['SUBMITTED','PENDING','RUNNABLE','STARTING','RUNNING']:
            yield from self.get_jobs_with_status(status)

    def parse_key_datetime(self, str_datetime):
        ## Airflow sometimes uses microseconds, but not consistently
        try:
            return datetime.strptime(str_datetime, '%Y-%m-%dT%H:%M:%S.%f')
        except:
            return datetime.strptime(str_datetime, '%Y-%m-%dT%H:%M:%S')

    def start(self):
        self.dagbag = models.DagBag(settings.DAGS_FOLDER)

        self.batch_client = boto3.client('batch')

        self.job_queue = configuration.get('aws_batch', 'JOB_QUEUE')
        self.default_job_definition = configuration.get('aws_batch', 'DEFAULT_JOB_DEFINITION')

        for job in self.get_jobs():
            if job['status'] in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']:
                params = job['parameters']
                start_date = self.parse_key_datetime(params['start_date'])
                key = (params['dag_id'], params['task_id'], start_date,)
                self.running[key] = ''
                self.tasks[job['jobId']] = key
                self.last_state[job['jobId']] = job['status']

    def has_task(self, task_instance):
        if task_instance.key in self.queued_tasks or task_instance.key in self.running:
            return True

    def change_state(self, key, state):
        self.running.pop(key)
        self.event_buffer[key] = state

    def sync(self):
        for job_ids in batch(list(self.tasks.keys()), 100):
            response = self.batch_client.describe_jobs(jobs=job_ids)
            for job in response['jobs']:
                job_id = job['jobId']
                state = job['status']
                if self.last_state[job_id] != state:
                    if state == 'SUCCEEDED':
                        self.success(self.tasks[job_id])
                        del self.tasks[job_id]
                        del self.last_state[job_id]
                    elif state == 'FAILED':
                        self.fail(self.tasks[job_id])
                        del self.tasks[job_id]
                        del self.last_state[job_id]
                    else:
                        self.last_state[job_id] = state


    def execute_async(self, key, command, queue=None):
        dag = self.dagbag.get_dag(key[0])
        task = dag.get_task(key[1])

        if 'job_definition' in task.params:
            job_definition = task.params['job_definition']
        else:
            job_definition = self.default_job_definition

        start_date = key[2].isoformat()
        start_date_job_name = key[2].isoformat().replace(':','-').replace('.','-')

        response = self.batch_client.submit_job(
            jobName='{}__{}__{}'.format(dag.dag_id, task.task_id, start_date_job_name),
            jobQueue=self.job_queue,
            jobDefinition=job_definition,
            parameters={
                'dag_id': dag.dag_id,
                'task_id': task.task_id,
                'start_date': start_date
            },
            containerOverrides={
                'environment': [
                    {
                        'name': 'AIRFLOW_DAG_ID',
                        'value': dag.dag_id
                    },
                    {
                        'name': 'AIRFLOW_TASK_ID',
                        'value': task.task_id
                    },
                    {
                        'name': 'AIRFLOW_START_DATE',
                        'value': start_date
                    }
                ]
            })

        ## TODO: check response for success?

        job_id = response['jobId']
        self.tasks[job_id] = key
        self.last_state[job_id] = 'SUBMITTED'

    def end(self):
        self.sync()

    def terminate(self):
        pass

class AWSBatchPlugin(AirflowPlugin):
    name = "aws_batch_plugin"
    executors = [AWSBatchExecutor]
