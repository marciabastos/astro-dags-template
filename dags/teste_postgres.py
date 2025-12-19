from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def testar_postgres():
    hook = PostgresHook(postgres_conn_id="postgres")
    resultado = hook.get_first("SELECT 1;")
    print("Resultado do teste:", resultado)

with DAG(
    dag_id="teste_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "postgres"],
) as dag:

    PythonOperator(
        task_id="testar_conexao_postgres",
        python_callable=testar_postgres,
    )

