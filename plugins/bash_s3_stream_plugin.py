import os
import signal
import logging
import re
from subprocess import Popen, STDOUT

from smart_open import smart_open
import boto
import boto3

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

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

        super(BashOperator, self).__init__(input_file=None, output_file=None, *args, **kwargs)
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

                with fopen(self.input_file) as input_file:
                    with fopen(self.output_file) as out: 
                        sp = Popen(
                            ['bash', fname],
                            stdin=input_file,
                            stdout=out,
                            stderr=STDOUT,
                            cwd=tmp_dir,
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
