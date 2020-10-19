"""
This script provides the dag for the airflow scheduler, to be 
run quarterly. 
"""


# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'tarriq',
    'start_date': datetime(2020, 10, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@quarterly',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}