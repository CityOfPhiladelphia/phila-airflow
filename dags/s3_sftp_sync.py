from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import SlackNotificationOperator

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    'on_failure_callback': SlackNotificationOperator.failed(),
}

dag = DAG('s3_sftp_sync',
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval='@hourly',
    concurrency=1,
    max_active_runs=1,
    default_args=default_args
)

# Uses https://github.com/CityOfPhiladelphia/s3-sftp-sync
sync = BashOperator(
    task_id='sync',
    dag=dag,
    bash_command='source <(eastern_state load_environment "$EASTERN_STATE_BUCKET" "$EASTERN_STATE_NAME" "$EASTERN_STATE_ENV") && s3_sftp_sync'
)
