from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime

dag = DAG(dag_id="dag_concurrency",
          start_date=datetime(2023,7,1),
          schedule="@daily",
          catchup=True,
          max_active_runs=5)

start = EmptyOperator(dag=dag, task_id="start")
end = EmptyOperator(dag=dag, task_id="end")

extract = BashOperator(dag=dag, task_id="extract", bash_command="sleep 5")
clean = BashOperator(dag=dag, task_id="clean", bash_command="sleep 5")
load = EmptyOperator(dag=dag, task_id="load")

start >> extract >> clean >> load >> end