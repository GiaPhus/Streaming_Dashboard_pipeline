from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

from crawl_data import get_user_with_purchase
from insert_into_mysql import insert_record_to_mysql
from datetime import datetime, timedelta

def crawl_and_insert():
    start_time = time.time()
    while time.time() - start_time  < 10:
        records = get_user_with_purchase()
        insert_record_to_mysql(records)
        
default_args = {
    "owner" : "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    'insert_staging_data',
    default_args = default_args,
    description = 'Crawl_Data',
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 9, 25),
     catchup=False,
    tags=["crawl", "mysql"],
) as dag:

    task_crawl_insert = PythonOperator(
        task_id="crawl_and_insert",
        python_callable=crawl_and_insert
    )
