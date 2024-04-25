import datetime as dt
import logging

from airflow.decorators import dag, task

from dwh.airflow.dags.config import DependencyConfig
from dwh.core.domain.titanic import TitanicPassengersDownloadJob, calculate_sex_dm, create_titanic_table

args = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 9, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}


@dag(
    default_args=args,
    schedule_interval="0/15 * * * *",
    catchup=False,
    tags=["demo", "stg", "cdm", "titanic"],
    is_paused_upon_creation=False,
)
def titanic_dag():
    @task()
    def start_task():
        logging.info("Here we start!")

    @task()
    def create_titanic_table_task():
        logging.info("Creating titanic table")

        create_titanic_table(DependencyConfig.db_connection())

        logging.info("Titanic table created")

    @task()
    def download_titanic_dataset_task():
        logging.info("Downloading titanic dataset")

        job = TitanicPassengersDownloadJob(
            DependencyConfig.Connectors.titanic_passenger_connector(), DependencyConfig.Repository.titanic_passenger_repository()
        )

        job.download_titanic_dataset()

        logging.info("Downloaded titanic dataset")

    @task()
    def create_titanic_sex_dm_task():
        logging.info("Calculating Sex DM")

        calculate_sex_dm(DependencyConfig.db_connection())

        logging.info("Sex DM created")

    start = start_task()
    create_table = create_titanic_table_task()
    download_titanic_dataset = download_titanic_dataset_task()
    create_titanic_sex_dm = create_titanic_sex_dm_task()

    start >> create_table >> download_titanic_dataset >> create_titanic_sex_dm  # type: ignore


titanic_dag()
