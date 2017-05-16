import os
import signal
import logging
import re
import sys
from subprocess import Popen, PIPE
from tempfile import gettempdir, NamedTemporaryFile
from threading import Thread

from smart_open import smart_open
import boto
import boto3

from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory

s3_regex = r'^s3://([^/]+)/(.+)'

def fopen(file, mode='r'):
    # HACK: get boto working with instance credentials via boto3
    match = re.match(s3_regex, file)
    if match != None:
        client = boto3.client('s3')
        s3_connection = boto.connect_s3(
            aws_access_key_id=client._request_signer._credentials.access_key,
            aws_secret_access_key=client._request_signer._credentials.secret_key,
            security_token=client._request_signer._credentials.token)
        bucket = s3_connection.get_bucket(match.groups()[0])
        if mode == 'w':
            file = bucket.get_key(match.groups()[1], validate=False)
        else:
            file = bucket.get_key(match.groups()[1])
    return smart_open(file, mode=mode)

def pipe_stream(stream1, stream2):
    def stream_helper(stream1, stream2):
        for line in iter(stream1.readline, b''):
            stream2.write(line)
        stream2.close()

    t = Thread(target=stream_helper, args=(stream1, stream2))
    t.daemon = True
    t.start()

class BashStreamOperator(BaseOperator):
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
            input_file=None,
            output_file=None,
            *args, **kwargs):

        super(BashStreamOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.input_file = input_file
        self.output_file = output_file

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        bash_command = self.bash_command
        logging.info("tmp dir root location: \n" + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(bash_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))
                logging.info("Running command: " + bash_command)

                input_file = None
                if self.input_file:
                    input_file = fopen(self.input_file)

                out = None
                if self.output_file:
                    out = fopen(self.output_file, mode='w')

                ON_POSIX = 'posix' in sys.builtin_module_names

                sp = Popen(
                    ['bash', fname],
                    stdin=PIPE if input_file else None,
                    stdout=PIPE if out else None,
                    stderr=PIPE,
                    cwd=tmp_dir,
                    env=self.env,
                    preexec_fn=os.setsid,
                    bufsize=1,
                    close_fds=ON_POSIX)

                self.sp = sp

                if input_file:
                    pipe_stream(input_file, sp.stdin)

                if out:
                    pipe_stream(sp.stdout, out)

                for line in iter(sp.stderr.readline, b''):
                    logging.info(line)

                sp.wait()

                if input_file:
                    input_file.read_key.close(fast=True)

                logging.info("Command exited with "
                             "return code {0}".format(sp.returncode))

                if sp.returncode:
                    raise AirflowException("Bash command failed")

    def on_kill(self):
        logging.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)

class DatumPlugin(AirflowPlugin):
    name = "bash_stream_plugin"
    operators = [BashStreamOperator]
