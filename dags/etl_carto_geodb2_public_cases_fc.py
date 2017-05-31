from airflow import DAG

from etl_carto_geodb2_base import carto_geodb2_dag_factory

dag = carto_geodb2_dag_factory('GIS_311',
                               'public_cases_fc',
                               's3://"$S3_SCHEMA_BUCKET"/public_cases_fc.json',
                               geometry_support='sde-char',
                               schedule_interval='0 7 * * *'
                               #retries=2
                               )
