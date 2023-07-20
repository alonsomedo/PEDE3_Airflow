from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG(dag_id="example_01",
         start_date=datetime(2023,7,19),
         catchup=False,
         schedule=None
        ) as dag:

    extract = EmptyOperator(task_id="extract")

    transform = BashOperator(task_id="transform",
                             bash_command="sleep 10")

    load = BashOperator(task_id="load",
                        bash_command="sleep 5; echo 'The process hash finished'")

    extract >> transform >> load