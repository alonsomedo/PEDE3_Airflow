from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG(dag_id="catchup",
         start_date=datetime(2023,7,1),
         catchup=True,
         schedule='@daily' #schedule and schedule_interval are available, but schedule_interval is going to be deprecated.
        ) as dag:

    extract = EmptyOperator(task_id="extract")

    transform = BashOperator(task_id="transform",
                             bash_command="sleep 2")

    load = BashOperator(task_id="load",
                        bash_command="sleep 2; echo 'The process hash finished'")

    extract >> transform >> load

# Docker command for connecting to the worker
# docker exec -ti <container-id/container-name> //bin/bash ---> Linux
# docker exec -ti <container-id/container-name> /bin/bash ---> PowerShell-Windows

# Command to run backfills
# airflow dags backfill -s <start-date> -e <end-date> <dag_id>
# airflow dags backfill -s 2023-06-01 -e 2023-06-30 backfill