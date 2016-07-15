import logging

from airflow.plugins_manager import AirflowPlugin

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.ftp_hook import FTPHook, FTPSHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from os.path import exists


class FileTransferOperator(BaseOperator):
    """
    Downloads a file from a connection.

    :param conn_id: The connection to run the operator against.
    :type conn_id: string
    :param remote_path: The path to the file on the server
    :type remote_path: string
    :param local_path: The full path to which the file will be downloaded
    :type local_path: string
    :param secure: Whether to use a secure connection
    :type secure: bool
    """

    template_fields = ('endpoint','data',)
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 remote_path,
                 local_path,
                 secure=False, *args, **kwargs):
        super(FileTransferOperator, self).__init__(*args, **kwargs)
        self.conn_id = http_conn_id
        self.remote_path = remote_path
        self.local_path = local_path
        self.secure = secure

    def execute(self, context):
        Hook = (FTPSHook if self.secure else FTPHook)
        conn = Hook(ftp_conn_id=self.conn_id)
        logging.info("Retrieving file from FTP server")
        conn.retrieve_file(self.remote_path, self.local_path)


class FileTransferOperatorPlugin(AirflowPlugin):
    name = "file_transfer_operator_plugin"
    operators = [FileTransferOperator]
