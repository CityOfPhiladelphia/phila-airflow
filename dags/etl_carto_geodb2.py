from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators import TheELOperator
from airflow.operators import SlackNotificationOperator

def carto_geodb2_dag_factory(geodb2_schema,
                            table_name,
                            schema_file,
                            geometry_support=None,
                            geodb2_table_name=None, # defaults to same as table_name
                            final_carto_table_name=None, # overides final carto table - useful for testing like test_table
                            schedule_interval='0 7 * * *', # defaults to 7am UTC (2am EST)
                            retries=0):
    dag_id = 'etl_carto_geodb2_{}'.format(table_name)

    default_args = {
        'owner': 'airflow',
        'on_failure_callback': SlackNotificationOperator.failed(),
        'retries': retries
    }

    dag = DAG(dag_id,
        start_date=datetime.now() - timedelta(days=1),
        schedule_interval=schedule_interval,
        default_args=default_args
    )

    data_file = 's3://"$S3_STAGING_BUCKET"/' + dag_id + '/{{run_id}}/' + dag_id + '.csv'

    extract_from_geodb2 = TheELOperator(
        task_id='extract_{}'.format(table_name),
        dag=dag,
        el_command='read',
        db_schema=geodb2_schema,
        table_name=geodb2_table_name or table_name,
        geometry_support= geometry_support,
        connection_string='"$GEODB2_CONN_STRING"',
        output_file=data_file
    )

    postgis_geometry_support = None
    if geometry_support != None:
        postgis_geometry_support = 'postgis'

    create_temp_carto_table = TheELOperator(
        task_id='create_temp_table_' + table_name,
        dag=dag,
        el_command='create_table',
        db_schema='phl',
        table_name=table_name + '_{{run_id.lower()}}',
        table_schema_path=schema_file,
        geometry_support= postgis_geometry_support,
        connection_string='"$CARTO_CONN_STRING"'
    )

    load_to_temp_carto_table = TheELOperator(
        task_id='load_' + table_name,
        dag=dag,
        el_command='write',
        db_schema='phl',
        table_name=table_name + '_{{run_id.lower()}}',
        skip_headers=True,
        table_schema_path=schema_file,
        geometry_support= postgis_geometry_support,
        connection_string='"$CARTO_CONN_STRING"',
        input_file=data_file
    )

    swap_and_real_tables = TheELOperator(
        task_id='swap_' + table_name,
        dag=dag,
        el_command='swap_table',
        db_schema='phl',
        new_table_name=table_name + '_{{run_id.lower()}}',
        old_table_name=final_carto_table_name or table_name,
        connection_string='"$CARTO_CONN_STRING"'
    )

    extract_from_geodb2 >> create_temp_carto_table >> load_to_temp_carto_table >> swap_and_real_tables

    globals()[dag_id] = dag # Airflow looks at the module global vars for DAG type variables

carto_geodb2_dag_factory('GIS_OPA',
                         'assessments',
                         's3://"$S3_SCHEMA_BUCKET"/opa_assessments.json',
                         schedule_interval='0 6 * * *'
                         #retries=2
                         )

# carto_geodb2_dag_factory('GIS_ODDT',
#                          'opa_properties_public',
#                          's3://"$S3_SCHEMA_BUCKET"/opa_properties_public.json',
#                          geometry_support='sde-char',
#                          schedule_interval='0 6 * * *',
#                          final_carto_table_name='awm_opa_properties_public')

carto_geodb2_dag_factory('GIS_311',
                         'public_cases_fc',
                         's3://"$S3_SCHEMA_BUCKET"/public_cases_fc.json',
                         geometry_support='sde-char',
                         schedule_interval='0 7 * * *'
                         #retries=2
                         )

# carto_geodb2_dag_factory('GIS_POLICE',
#                          'incidents_part1_part2',
#                          's3://"$S3_SCHEMA_BUCKET"/incidents_part1_part2.json',
#                          geometry_support='sde-char',
#                          schedule_interval='0 7 * * *',
#                          final_carto_table_name='awm_incidents_part1_part2')

carto_geodb2_dag_factory('GIS_LNI',
                         'li_imm_dang',
                         's3://"$S3_SCHEMA_BUCKET"/li_imm_dang.json',
                         geometry_support='sde-char',
                         schedule_interval='0 7 * * *',
                         final_carto_table_name='awm_li_imm_dang')

carto_geodb2_dag_factory('GIS_LNI',
                         'li_trade_licenses',
                         's3://"$S3_SCHEMA_BUCKET"/li_trade_licenses.json',
                         schedule_interval='0 7 * * *',
                         final_carto_table_name='awm_li_trade_licenses')

carto_geodb2_dag_factory('GIS_STREETS',
                         'wastebaskets_big_belly',
                         's3://"$S3_SCHEMA_BUCKET"/wastebaskets_big_belly.json',
                         geometry_support='sde-char',
                         schedule_interval='0 7 * * *',
                         final_carto_table_name='awm_wastebaskets_big_belly')

carto_geodb2_dag_factory('GIS_WATERSHEDS',
                         'stormwater_grants',
                         's3://"$S3_SCHEMA_BUCKET"/stormwater_grants.json',
                         geometry_support='sde-char',
                         schedule_interval='0 7 * * *',
                         final_carto_table_name='awm_stormwater_grants')

carto_geodb2_dag_factory('GIS_ODDT',
                         'employee_salaries',
                         's3://"$S3_SCHEMA_BUCKET"/employee_salaries.json',
                         schedule_interval='0 7 * * *',
                         final_carto_table_name='awm_employee_salaries')
