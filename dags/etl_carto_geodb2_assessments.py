from airflow import DAG

from etl_carto_geodb2_base import carto_geodb2_dag_factory

dag = carto_geodb2_dag_factory('GIS_OPA',
                               'assessments',
                               's3://"$S3_SCHEMA_BUCKET"/opa_assessments.json',
                               schedule_interval='0 6 * * *'
                               #retries=2
                               )
