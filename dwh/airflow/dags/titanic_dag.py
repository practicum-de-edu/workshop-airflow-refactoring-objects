import datetime as dt
import logging

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dwh.airflow.dags.config import DependencyConfig
from dwh.core.domain.titanic import TitanicPassengersDownloadJob, calculate_sex_dm, create_titanic_table

args = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 9, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}


def create_titanic_table_task():
    logging.info("Creating titanic table")

    create_titanic_table(DependencyConfig.db_connection())

    logging.info("Titanic table created")


def download_titanic_dataset_task():
    logging.info("Downloading titanic dataset")

    job = TitanicPassengersDownloadJob(
        DependencyConfig.Connectors.titanic_passenger_connector(), DependencyConfig.Repository.titanic_passenger_repository()
    )

    job.download_titanic_dataset()

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

titanic_sex_dm = PythonOperator(
    task_id="create_titanic_table",
    python_callable=create_titanic_table_task,
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
