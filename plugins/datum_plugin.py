import logging

import csv
import datum

from airflow.plugins_manager import AirflowPlugin

from airflow.exceptions import AirflowException
from airflow.hooks import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


def get_chunk(iterator, size):
    for _ in range(size):
        yield next(iterator)

def chunks(iterable, size):
    iterator = iter(iterable)
    while True:
        yield get_chunk(iterator, size)


class DatumHook (BaseHook):
    def __init__(self, db_conn_id, conn=None):
        self.db_conn_id = db_conn_id
        self.conn = conn

    def get_conn(self):
        SCHEMAS = {
            'postgres': 'postgis',
            'oracle': 'oracle-stgeom',
        }

        if self.conn is None:
            params = self.get_connection(self.db_conn_id)

            if params.conn_type not in SCHEMAS:
                raise AirflowException('Could not create Datum connection for connection type {}'.format(params.conn_type))

            logging.info('Establishing connection to {}'.format(self.db_conn_id))
            auth = params.login + ':' + params.password
            conn_string = SCHEMAS[params.conn_type] + '://' + auth + '@' + params.extra
            logging.info('Connection string is {}'.format(conn_string))
            self.conn = datum.connect(conn_string)
        return self.conn


class DatumCSV2TableOperator(BaseOperator):
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

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 db_table_name,
                 csv_path,
                 truncate=True,
                 db_field_overrides=None,
                 sql_override=None,
                 *args, **kwargs):
        super(DatumCSV2TableOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table_name = db_table_name
        self.csv_path = csv_path
        self.truncate = truncate
        self.db_field_overrides = db_field_overrides or {}
        self.sql_override = sql_override

    def execute(self, context):
        logging.info("Connecting to the database")
        self.hook = DatumHook(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()

        table = self.db_table_name

        with open(self.csv_path) as csvfile:
            rows = csv.DictReader(csvfile)

            self.create_table_if_not_exist(rows.fieldnames)
            self.truncate_table()

            logging.info("Inserting rows into table, 1000 at a time")
            for chunk in chunks(rows, 1000):
                self.conn.table(table).write(list(chunk))

        logging.info("Done!")

    def create_table_if_not_exist(self, fieldnames):
        table = self.db_table_name

        logging.info('Checking whether table {} exists'.format(table))
        if table.upper() in self.conn.tables:
            logging.info('Table {} already exists.'.format(table))
            return

        fielddefs = ',\n'.join(
            '"{}" {}'.format(fieldname.lower(), self.db_field_overrides.get(fieldname, 'VARCHAR2(4000)'))
            for fieldname in fieldnames
        )
        sql = self.sql_override or 'CREATE TABLE {} ({})'.format(table, fielddefs)

        logging.info('Creating the table: {}'.format(sql))
        self.conn.execute(sql)

    def truncate_table(self):
        table = self.db_table_name

        if self.truncate:
            logging.info("Truncating the {} table".format(table))
            self.conn._child.truncate(table)


class DatumPlugin(AirflowPlugin):
    name = "datum_plugin"
    operators = [DatumCSV2TableOperator]
    hooks = [DatumHook]
