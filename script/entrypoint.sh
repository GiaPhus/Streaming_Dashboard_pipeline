#!/bin/bash
set -e
# chmod +x ./script/entrypoint.sh

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email trangiaphu25092003@gmail.com \
    --password admin
fi

$(command -v airflow) db upgrade

exec airflow webserver