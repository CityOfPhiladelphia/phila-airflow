#!/usr/bin/env bash

alias python=python3
alias pip=pip3

AIRFLOW_HOME="/usr/local/airflow"
CMD="airflow"

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

if [ "$1" = "webserver" ]; then
  echo "Initialize database..."
  $CMD initdb
  exec $CMD webserver
else
  exec $CMD "$@"
fi
