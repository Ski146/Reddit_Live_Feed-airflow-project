from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from reddit_etl import run_reddit_etl
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2023, 10, 1)
}

with DAG(
    dag_id='reddit_etl_dag',
    default_args=default_args,
    description='A simple ETL DAG for Reddit data'
) as dag:

    run_etl = PythonOperator(
        task_id='run_reddit_etl',
        python_callable=run_reddit_etl,
        dag=dag
    )

    run_etl

