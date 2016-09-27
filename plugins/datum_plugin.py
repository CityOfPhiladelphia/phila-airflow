import logging

import csv
import datum

from airflow.plugins_manager import AirflowPlugin

from airflow.exceptions import AirflowException
from airflow.hooks import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


def lower_keys_dict(d):
    lower_d = {}
    for k in d:
        lower_d[k.lower()] = d[k]
    return lower_d

class DatumHook (BaseHook):
    SCHEMAS = {
        'postgres': 'postgis',
        'oracle': 'oracle-stgeom',
    }

    def __init__(self, db_conn_id, conn=None):
        self.db_conn_id = db_conn_id
        self.conn = conn
        self.conn_str = None

    def get_conn_str(self):
        if self.conn_str is None:
            params = self.get_connection(self.db_conn_id)
            self.conn_str = '{schema}://{auth}@{host}{port}{path}'.format(
                schema=self.SCHEMAS[params.conn_type],
                auth=params.login + ':' + params.password,
                host=params.host,
                port=(':' + str(params.port) if params.port else ''),
                path=('/' + params.schema if params.schema else '')
            )
            logging.info(self.conn_str)
        return self.conn_str

    def get_conn(self):
        if self.conn is None:
            params = self.get_connection(self.db_conn_id)

            if params.conn_type not in self.SCHEMAS:
                raise AirflowException('Could not create Datum connection for connection type {}'.format(params.conn_type))

            logging.info('Establishing connection to {}'.format(self.db_conn_id))
            conn_string = self.get_conn_str()
            self.conn = datum.connect(conn_string)
        return self.conn


class DatumLoadOperator(BaseOperator):
    """
    Load a CSV file into a database table

    * Where does the CSV come from? Filename or stream
    * What's the database connection info?
    * What table should it go in?

    :param db_conn_id: The connection to run the operator against.
    :type db_conn_id: string
    :param db_table_name: The name of the table to load into.
    :type db_table_name: string
    :param csv_path: The path to the file on the server
    :type csv_path: string
    :param truncate: Whether the table should be truncated before inserting.
    :type truncate: bool
    """

    template_fields = ('db_table_name','csv_path',)
    ui_color = '#88ccff'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 db_table_name,
                 csv_path,
                 truncate=True,
                 db_field_overrides=None,
                 sql_override=None,
                 *args, **kwargs):
        super(DatumLoadOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table_name = db_table_name
        self.csv_path = csv_path
        self.truncate = truncate
        self.db_field_overrides = db_field_overrides or {}
        self.sql_override = sql_override

    def execute(self, context):
        logging.info("Connecting to the database {}".format(self.db_conn_id))
        self.hook = DatumHook(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()

        with open(self.csv_path, 'rU') as csvfile:
            if self.truncate:
                self.truncate_table()
            self.load_table(csvfile, chunk_size=1000)

        logging.info("Done!")

    def truncate_table(self):
        table_name = self.db_table_name

        logging.info("Truncating the {} table".format(table_name))
        table = self.conn.table(table_name)
        table.delete()

    def load_table(self, csvfile, chunk_size=None):
        table_name = self.db_table_name

        logging.info("Loading data into the {} table{}".format(
            table_name, ', {} rows at a time'.format(chunk_size) if chunk_size else ''))
        table = self.conn.table(table_name)
        table.load(csvfile, chunk_size=chunk_size)


class DatumExecuteOperator(BaseOperator):
    """
    Execute a SQL command

    :param db_conn_id: The connection to run the operator against.
    :type db_conn_id: string
    :param db_table_name: The name of the table to load into.
    :type db_table_name: string
    :param sql: The sql command to run
    :type sql: string
    """

    template_fields = ('sql',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 sql,
                 *args, **kwargs):
        super(DatumExecuteOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.sql = sql

    def execute(self, context):
        logging.info("Connecting to the database {}".format(self.db_conn_id))
        self.hook = DatumHook(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()

        logging.info("Running the SQL command: {}".format(self.sql))
        self.conn.execute(self.sql)
        logging.info("Done!")


class DatumPlugin(AirflowPlugin):
    name = "datum_plugin"
    operators = [DatumExecuteOperator, DatumLoadOperator]
    hooks = [DatumHook]
