import datetime as dt
import logging

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dwh.airflow.dags.config import DependencyConfig
from dwh.core.domain.titanic import calculate_sex_dm, download_titanic_dataset

args = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 9, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}


def download_titanic_dataset_task():
    logging.info("Downloading titanic dataset")

    download_titanic_dataset(DependencyConfig.titanic_source_url(), DependencyConfig.db_connection())

    logging.info("Downloaded titanic dataset")


def create_titanic_sex_dm_task():
    logging.info("Calculating Sex DM")

    calculate_sex_dm(DependencyConfig.db_connection())

    logging.info("Sex DM created")


dag = DAG(
    dag_id="titanic_dag",
    schedule_interval="0/15 * * * *",
    start_date=dt.datetime(2024, 4, 1),
    catchup=False,
    tags=["demo", "stg", "cdm", "titanic"],
    is_paused_upon_creation=False,
    default_args=args,
)


start = BashOperator(
    task_id="start",
    bash_command='echo "Here we start! "',
    dag=dag,
)

create_titanic_dataset = PythonOperator(
    task_id="download_titanic_dataset",
    python_callable=download_titanic_dataset_task,
    dag=dag,
)

titanic_sex_dm = PythonOperator(
    task_id="create_titanic_sex_dm",
    python_callable=create_titanic_sex_dm_task,
    dag=dag,
)


start >> create_titanic_dataset >> titanic_sex_dm
