from datetime import timedelta

#import sys
#sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
#from scripts import myscript_cus
import os
import datetime 
from airflow import DAG
from airflow import models
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta



with DAG(
    'postgres_to_bq',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        #'email': ['airflow@example.com'],
        #'email_on_failure': False,
        #'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'schedule_interval' : timedelta(days=1)
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='A simple tutorial DAG',
    #schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
      task1=BashOperator(
		task_id='Postgres_via_python',
		bash_command="python /home/airflow/gcs/dags/myscript_cus.py"
	)
      task1
    