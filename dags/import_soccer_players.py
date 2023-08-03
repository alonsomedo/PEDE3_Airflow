import json
import requests
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from airflow.utils.edgemodifier import Label
from airflow.models import Variable

from datetime import datetime, timedelta
from pandas import json_normalize


with DAG(
    dag_id='import_soccer_players',
    start_date=datetime(2023, 7, 26),
    tags=["soccer", "api"],
    schedule="@daily"
) as dag:
    
    def get_soccer_players(team_id):
        url = 'https://v3.football.api-sports.io/players/squads'
        params = {'team': team_id}
        headers = {
            'x-rapidapi-host': "v3.football.api-sports.io",
            'x-rapidapi-key': Variable.get('soccer_secret_key')
        }
        response = requests.get(url=url, params=params, headers=headers)
        response = json.loads(response.text)["response"][0]["players"]
        df = json_normalize(response)
        #df.to_csv(f'/opt/airflow/dags/input_data/{team_id}.csv', index=False, header=False, sep="\t")
        df.to_csv(f'/opt/airflow/dags/input_data/{team_id}.csv', index=False, header=False)
        logging.info(f"The file for soccer team with id: {team_id} was exported successfully.")

    def load_soccer_players(team_id):
        # This command needs to be run in MySQL: SET GLOBAL local_infile=1;
        file_path = f'/opt/airflow/dags/input_data/{team_id}.csv'
        mysql_hook = MySqlHook(mysql_conn_id="mysql_default", local_infile=True)
        #mysql_hook.bulk_load('soccer_players', file_path)
        mysql_hook.bulk_load_custom('soccer_players', file_path, extra_options="FIELDS TERMINATED BY ','")
        logging.info("Data was loaded successfully!")


    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    get_players = PythonOperator(
        task_id="get_players",
        python_callable=get_soccer_players,
        op_args=[529]
    )

    create_table = MySqlOperator(
        task_id="create_table_if_not_exists",
        mysql_conn_id="mysql_default",
        database="dwh",
        sql="""
            CREATE TABLE IF NOT EXISTS soccer_players
            (
                player_id int,
                name varchar(100),
                age int,
                number int,
                position varchar(100),
                photo varchar(200)
            )
            """
    )

    load_players = PythonOperator(
        task_id="load_players",
        python_callable=load_soccer_players,
        op_kwargs={
            "team_id": 529
        }
    )

    # start >> Label("verify") >> create_table >> get_players >> end
    start >> create_table >> get_players >> load_players >> end