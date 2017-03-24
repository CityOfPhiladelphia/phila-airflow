import os
import signal
import logging
from subprocess import Popen, STDOUT, PIPE

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class BashPlusOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed.
    :type bash_command: string
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    """
    template_fields = ('bash_command', 'env')
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            bash_command,
            env=None,
            *args, **kwargs):

        super(BashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        bash_command = self.bash_command

        ## https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.upload_fileobj
        ## https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.download_fileobj
        ## TODO: optionally pipe from s3 to stdin 
        ## TODO: optionally pipe stdout to s3 
        ## TODO: pipe stderr to logger, adding dag_id, task_id, and task_instance_id - in context?

        logging.info("Running command: " + bash_command)
        sp = Popen(
            ['bash', '-c', bash_command], ## maybe use tmp file like they did? cleaner passing
            stdout=PIPE,
            stderr=STDOUT,
            env=self.env,
            preexec_fn=os.setsid)

        self.sp = sp

        logging.info("Output:")
        line = ''
        for line in iter(sp.stdout.readline, b''):
            line = line.decode(self.output_encoding).strip()
            logging.info(line)
        sp.wait()
        logging.info("Command exited with "
                     "return code {0}".format(sp.returncode))

        if sp.returncode:
            raise AirflowException("Bash command failed")

    def on_kill(self):
        logging.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
