#!/usr/bin/env bash

alias python=python3
alias pip=pip3

AIRFLOW_HOME="/usr/local/airflow"
CMD="airflow"
: ${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")}

# Install custom python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(which pip) install --user -r /requirements.txt
fi

if [ -z ${EASTERN_STATE_BUCKET+x} ]; then
  echo "$(date) - Not using eastern_state"
else
  echo "$(date) - Installing environment variables using eastern_state"
  source <(eastern_state download "$EASTERN_STATE_BUCKET" "$EASTERN_STATE_NAME" | \
           eastern_state decrypt | \
           eastern_state exports "$EASTERN_STATE_ENV")
fi

# Generate Fernet key
sed -i "s|\$FERNET_KEY|$FERNET_KEY|" "$AIRFLOW_HOME"/airflow.cfg

if [ "$1" = "webserver" ]; then
  echo "Initialize database..."
  $CMD initdb
fi

if [ "x$EXECUTOR" = "xLocal" ]
then
  sed -i "s/executor = CeleryExecutor/executor = LocalExecutor/" "$AIRFLOW_HOME"/airflow.cfg
  exec $CMD "$@"
else
  if [ "$1" = "version" ]; then
    exec $CMD version
  fi
  sed -i "s/executor = CeleryExecutor/executor = SequentialExecutor/" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = sqlite:////usr/local/airflow/airflow.db#" "$AIRFLOW_HOME"/airflow.cfg
  echo "Initialize database..."
  $CMD initdb
  exec $CMD webserver
fi