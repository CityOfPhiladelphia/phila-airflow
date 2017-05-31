from airflow import DAG

from etl_carto_geodb2_base import carto_geodb2_dag_factory

dag = carto_geodb2_dag_factory('GIS_ODDT',
                               'employee_salaries',
                               's3://"$S3_SCHEMA_BUCKET"/employee_salaries.json',
                               schedule_interval='0 7 * * *')
