from airflow import DAG

from etl_carto_geodb2_base import carto_geodb2_dag_factory

dag = carto_geodb2_dag_factory('GIS_POLICE',
                               'incidents_part1_part2',
                               's3://"$S3_SCHEMA_BUCKET"/incidents_part1_part2.json',
                               geometry_support='sde-char',
                               schedule_interval='0 7 * * *',
                               final_carto_table_name='awm_incidents_part1_part2')
