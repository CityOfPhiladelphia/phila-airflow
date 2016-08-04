import logging

import datum

from airflow.plugins_manager import AirflowPlugin

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CSV2DBOperator(BaseOperator):
    """
    Load a CSV file into a database table

    * Where does the CSV come from? Filename or stream
    * What's the database connection info?
    * What table should it go in?

    :param db_conn_id: The connection to run the operator against.
    :type db_conn_id: string
    :param csv_path: The path to the file on the server
    :type csv_path: string
    """

    template_fields = ('remote_path','local_path',)
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 remote_path,
                 local_path,
                 secure=False, *args, **kwargs):
        super(CSV2DBOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.remote_path = remote_path
        self.local_path = local_path
        self.secure = secure

    def execute(self, context):
        Hook = (FTPSHook if self.secure else FTPHook)
        conn = Hook(ftp_conn_id=self.conn_id)
        logging.info("Retrieving file from FTP server")
        conn.retrieve_file(self.remote_path, self.local_path)


class DatumPlugin(AirflowPlugin):
    name = "datum_plugin"
    operators = [CSV2DBOperator]
