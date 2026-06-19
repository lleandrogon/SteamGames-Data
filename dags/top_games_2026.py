from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pendulum
import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from bronze.steam import extract_games
from silver.steam import transform_silver_games
from gold.steam import transform_gold_games

with DAG(
    dag_id = "top_games_2026",
    schedule = "30 21 * * 5",
    max_active_runs = 1,
    default_args = {
        "owner": "airflow",
        "retries": 1,
        "start_date": pendulum.datetime(2026, 6, 19, tz = "America/Sao_Paulo")
    },
    template_searchpath = ["/opt/airflow/src/sql"],
    catchup = False,
    tags = ["steam", "games", "2026"]
) as dag:
    
    e_games = PythonOperator(
        task_id = "extract_games",
        python_callable = extract_games
    )

    t_silver_games = PythonOperator(
        task_id = "transform_silver_games",
        python_callable = transform_silver_games
    )

    t_gold_games = PythonOperator(
        task_id = "transform_gold_games",
        python_callable = transform_gold_games
    )

    ct_top_games_2026 = SQLExecuteQueryOperator(
        task_id = "create_table_top_games_2026",
        conn_id = "steam",
        sql = "create_table_top_games_2026.sql"
    )

    cui_app_id = SQLExecuteQueryOperator(
        task_id = "create_unique_index_app_id",
        conn_id = "steam",
        sql = "create_unique_index_top_games_2026.sql"
    )

    e_games >> t_silver_games >> t_gold_games >> ct_top_games_2026 >> cui_app_id