import logging

import csv
import datum

from airflow.plugins_manager import AirflowPlugin
from airflow.operators import BashOperator
from airflow.utils.decorators import apply_defaults

class TheELOperator(BashOperator):
    @apply_defaults
    def __init__(
            self,
            el_command=None,
            table_name=None,
            new_table_name=None,
            old_table_name=None,
            table_schema_path=None,
            connection_string=None,
            db_schema=None,
            geometry_support=None,
            indexes_fields=None,
            input_file=None,
            output_file=None,
            *args, **kwargs):

        bash_command = 'the_el {} {}'.format(el_command, table_name or new_table_name)

        if table_schema_path != None:
            if el_command == 'create_table':
                bash_command += ' {}'.format(table_schema_path)
            else:
                bash_command += ' --table-schema-path {}'.format(table_schema_path)

        if el_command == 'swap_table':
            bash_command += ' ' + old_table_name

        if connection_string != None:
            bash_command += ' --connection-string {}'.format(connection_string)

        if db_schema != None:
            bash_command += ' --db-schema {}'.format(db_schema)

        if geometry_support != None:
            bash_command += ' --geometry-support {}'.format(geometry_support)

        if indexes_fields != None:
            if isinstance(indexes_fields, list):
                _indexes_fields = ','.join(indexes_fields)
            else:
                _indexes_fields = indexes_fields

            bash_command += ' --indexes-fields {}'.format(_indexes_fields)

        if input_file != None:
            bash_command += ' --input-file {}'.format(input_file)

        if output_file != None:
            bash_command += ' --output-file {}'.format(output_file)

        kwargs['bash_command'] = bash_command

        super(TheELOperator, self).__init__(*args, **kwargs)

class TheELPlugin(AirflowPlugin):
    name = "the_el_plugin"
    operators = [TheELOperator]
