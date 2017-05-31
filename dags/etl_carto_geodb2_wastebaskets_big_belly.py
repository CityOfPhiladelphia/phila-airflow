from airflow import DAG

from etl_carto_geodb2_base import carto_geodb2_dag_factory

dag = carto_geodb2_dag_factory('GIS_STREETS',
                               'wastebaskets_big_belly',
                               's3://"$S3_SCHEMA_BUCKET"/wastebaskets_big_belly.json',
                               geometry_support='sde-char',
                               schedule_interval='0 7 * * *',
                               from_srid=2272,
                               to_srid=4326)
