from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

with DAG(dag_id="default_args",
         start_date=datetime(2023,7,24),
         catchup=False,
         schedule='@daily' #schedule and schedule_interval are available, but schedule_interval is going to be deprecated.
        ) as dag:

    extract = EmptyOperator(task_id="extract", 
                            owner="Squad_A",
                            depends_on_past=True)

    transform = BashOperator(task_id="transform",
                             bash_command="sleep 50",
                             owner="Squad_A",
                             execution_timeout=timedelta(seconds=30))

    load = BashOperator(task_id="load",
                        bash_command="exit 1",
                        owner="Squad_B",
                        retries=3,
                        retry_delay=timedelta(seconds=30))

    extract >> transform >> load