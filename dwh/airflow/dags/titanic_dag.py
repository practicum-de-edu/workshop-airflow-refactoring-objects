import datetime as dt
import logging

from airflow.hooks.base import BaseHook
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dwh.core.titanic import calculate_sex_dm, download_titanic_dataset

args = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 9, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}


def connection_url(
    host: str,
    port: str,
    db_name: str,
    user: str,
    pw: str,
    sslmode: str = "disable",
) -> str:
    return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
        host=host,
        port=port,
        db_name=db_name,
        user=user,
        pw=pw,
        sslmode=sslmode,
    )


def download_titanic_dataset_task():
    logging.info("Downloading titanic dataset")

    url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"

    connection = BaseHook.get_connection("POSTGRES_DB")
    conn_url = connection_url(
        connection.host,
        str(connection.port),
        connection.schema,
        connection.login,
        connection.password,
    )

    download_titanic_dataset(url, conn_url)

    logging.info("Downloaded titanic dataset")


def create_titanic_sex_dm_task():
    logging.info("Calculating Sex DM")

    connection = BaseHook.get_connection("POSTGRES_DB")
    conn_url = connection_url(
        connection.host,
        str(connection.port),
        connection.schema,
        connection.login,
        connection.password,
    )

    calculate_sex_dm(conn_url)

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
